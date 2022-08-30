package scan_test

import (
	"fmt"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
	"github.com/unhandled-exception/sophiadb/pkg/storage"
	"github.com/unhandled-exception/sophiadb/pkg/tx/transaction"
	"github.com/unhandled-exception/sophiadb/pkg/types"
)

var (
	_ scan.Scan       = &scan.TableScan{}
	_ scan.UpdateScan = &scan.TableScan{}
)

type TableScanTestSuite struct {
	Suite
}

func TestTableScanTestsuite(t *testing.T) {
	suite.Run(t, new(TableScanTestSuite))
}

func (ts *TableScanTestSuite) newSUT(testPath string) (*scan.TableScan, *transaction.Transaction, *storage.Manager, func()) {
	t := ts.T()

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, testPath)

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	sut, err := scan.NewTableScan(trx, testDataFile, ts.testLayout())
	require.NoError(t, err)

	return sut, trx, fm, func() {
		fm.Close()
	}
}

func (ts *TableScanTestSuite) TestGetAndSetValues() {
	t := ts.T()
	testPath := t.TempDir()

	wsut, wtx, fm, wsutClean := ts.newSUT(testPath)
	defer wsutClean()

	const blocks = 100
	cnt := (defaultTestBlockSize / wsut.Layout().SlotSize) * blocks

	require.NoError(t, wsut.BeforeFirst())

	// Заполняем странички
	for i := 0; i < int(cnt); i++ {
		require.NoErrorf(t, wsut.BeforeFirst(), "before first i == %d", i)
		require.NoErrorf(t, wsut.Insert(), "write insert i == %d", i)

		require.NoErrorf(t, wsut.SetInt64("id", int64(i+1)), "write int64 i == %d", i)
		require.NoErrorf(t, wsut.SetInt8("age", int8(i+2)), "write int8 i == %d", i)
		require.NoErrorf(t, wsut.SetString("name", fmt.Sprintf("user %d", i)), "write string i == %d", i)
	}

	wsut.Close()
	require.NoError(t, wtx.Commit())

	fLen, err := fm.Length(wsut.Filename)
	require.NoError(t, err)
	assert.EqualValues(t, blocks, fLen)

	rsut, rtx, fm, rsutClean := ts.newSUT(testPath)
	defer rsutClean()

	require.NoError(t, rsut.BeforeFirst())

	fLen, err = fm.Length(rsut.Filename)
	require.NoError(t, err)
	assert.EqualValues(t, blocks, fLen)

	// Сканируем таблицу
	for i := 0; i < int(cnt); i++ {
		ok, werr := rsut.Next()
		require.NoErrorf(t, werr, "read next i=%d", i)
		require.Truef(t, ok, "read next i=%d", i)

		idVal, werr := rsut.GetInt64("id")
		require.NoErrorf(t, werr, "read int64 i=%d", i)
		assert.EqualValues(t, int64(i+1), idVal)

		ageVal, werr := rsut.GetInt8("age")
		require.NoErrorf(t, werr, "read int8 i=%d", i)
		assert.EqualValues(t, int8(i+2), ageVal)

		nameVal, werr := rsut.GetString("name")
		require.NoErrorf(t, werr, "read string i=%d", i)
		assert.EqualValues(t, fmt.Sprintf("user %d", i), nameVal)
	}

	ok, err := rsut.Next()
	assert.NoError(t, err)
	assert.False(t, ok)

	rsut.Close()
	require.NoError(t, rtx.Commit())
}

func (ts *TableScanTestSuite) TestDelete() {
	t := ts.T()

	sut, tx, _, clean := ts.newSUT("")
	defer clean()
	defer func() {
		_ = tx.Commit()
	}()

	const blocks = 2
	cnt := (defaultTestBlockSize / sut.Layout().SlotSize) * blocks

	for i := 0; i < int(cnt); i++ {
		require.NoErrorf(t, sut.Insert(), "write insert i == %d", i)

		require.NoErrorf(t, sut.SetInt64("id", int64(i+1)), "write int64 i == %d", i)
		require.NoErrorf(t, sut.SetInt8("age", int8(i+2)), "write int8 i == %d", i)
		require.NoErrorf(t, sut.SetString("name", fmt.Sprintf("user %d", i)), "write string i == %d", i)
	}

	_ = sut.BeforeFirst()
	for i := 0; i < int(cnt-3); i++ {
		_, _ = sut.Next()
	}
	assert.Equal(t, types.RID{BlockNumber: 1, Slot: 31}, sut.RID())

	require.NoError(t, sut.Delete())

	_ = sut.BeforeFirst()
	require.NoError(t, sut.Insert())

	assert.Equal(t, types.RID{BlockNumber: 1, Slot: 31}, sut.RID())
}

func (ts *TableScanTestSuite) TestRID() {
	t := ts.T()

	sut, tx, _, clean := ts.newSUT("")
	defer clean()
	defer func() {
		_ = tx.Commit()
	}()

	const blocks = 2
	cnt := (defaultTestBlockSize / sut.Layout().SlotSize) * blocks

	for i := 0; i < int(cnt); i++ {
		require.NoErrorf(t, sut.Insert(), "write insert i == %d", i)

		require.NoErrorf(t, sut.SetInt64("id", int64(i+1)), "write int64 i == %d", i)
		require.NoErrorf(t, sut.SetInt8("age", int8(i+2)), "write int8 i == %d", i)
		require.NoErrorf(t, sut.SetString("name", fmt.Sprintf("user %d", i)), "write string i == %d", i)
	}

	_ = sut.BeforeFirst()

	assert.NoError(t, sut.MoveToRID(types.RID{
		BlockNumber: 1,
		Slot:        15,
	}))
	assert.Equal(t, types.RID{BlockNumber: 1, Slot: 15}, sut.RID())
}

func (ts *TableScanTestSuite) TestGetAndSetConstants() {
	t := ts.T()

	mc := minimock.NewController(t)

	sut, tx, _, clean := ts.newSUT("")
	defer clean()
	defer func() {
		_ = tx.Commit()
	}()

	const blocks = 4
	cnt := (defaultTestBlockSize / sut.Layout().SlotSize) * blocks

	for i := 0; i < int(cnt); i++ {
		require.NoErrorf(t, sut.Insert(), "write insert i == %d", i)

		require.NoErrorf(t, sut.SetVal("id", scan.NewInt64Constant(int64(i+1))), "write int64 i == %d", i)
		require.NoErrorf(t, sut.SetVal("age", scan.NewInt8Constant(int8(i+2))), "write int8 i == %d", i)
		require.NoErrorf(t, sut.SetVal("name", scan.NewStringConstant(fmt.Sprintf("user %d", i))), "write string i == %d", i)
	}

	require.NoError(t, sut.MoveToRID(types.RID{BlockNumber: 0, Slot: 0}))
	require.ErrorIs(t, sut.SetVal("id", scan.NewConstantMock(mc).ValueMock.Return(struct{}{})), scan.ErrTableScan)
	require.ErrorIs(t, sut.SetVal("age", scan.NewConstantMock(mc).ValueMock.Return(struct{}{})), scan.ErrTableScan)
	require.ErrorIs(t, sut.SetVal("name", scan.NewConstantMock(mc).ValueMock.Return(struct{}{})), scan.ErrTableScan)

	require.ErrorIs(t, sut.SetVal("unknown", scan.NewConstantMock(mc).ValueMock.Return(struct{}{})), scan.ErrTableScan)

	require.NoError(t, sut.BeforeFirst())

	for i := 0; i < int(cnt); i++ {
		_, err := sut.Next()
		require.NoErrorf(t, err, "wread next i == %d", i)

		idVal, err := sut.GetVal("id")
		require.NoErrorf(t, err, "read int64 i=%d", i)
		assert.EqualValues(t, int64(i+1), idVal.Value())

		ageVal, err := sut.GetVal("age")
		require.NoErrorf(t, err, "read int8 i=%d", i)
		assert.EqualValues(t, int8(i+2), ageVal.Value())

		nameVal, err := sut.GetVal("name")
		require.NoErrorf(t, err, "read string i=%d", i)
		assert.EqualValues(t, fmt.Sprintf("user %d", i), nameVal.Value())
	}

	_, err := sut.GetVal("unknown")
	require.ErrorIs(t, err, scan.ErrTableScan)
}

func (ts *TableScanTestSuite) TestHasField() {
	t := ts.T()

	sut, tx, _, clean := ts.newSUT("")
	defer clean()
	defer func() {
		_ = tx.Commit()
	}()

	assert.False(t, sut.HasField("unknown"))
}

func (ts *TableScanTestSuite) TestForeEachField_Ok() {
	t := ts.T()

	sut, _, _, clean := ts.newSUT("")
	defer clean()

	type fStruct struct {
		Name string
		Type records.FieldType
	}

	fields := make([]fStruct, 0, sut.Layout().Schema.Count())

	require.NoError(t, scan.ForEachField(sut, func(name string, fieldType records.FieldType) (bool, error) {
		fields = append(fields, fStruct{
			Name: name,
			Type: fieldType,
		})

		return false, nil
	}))

	assert.Equal(t,
		[]fStruct{
			{Name: "id", Type: records.Int64Field},
			{Name: "name", Type: records.StringField},
			{Name: "age", Type: records.Int8Field},
		},
		fields,
	)
}

func (ts *TableScanTestSuite) TestForeEachField_Errors() {
	t := ts.T()

	sut, _, _, clean := ts.newSUT("")
	defer clean()

	i := 0
	err := scan.ForEachField(sut, func(name string, fieldType records.FieldType) (bool, error) {
		i++

		return false, fmt.Errorf("fail caller %d", i)
	})
	require.EqualError(t, err, "fail caller 1")

	i = 0

	require.NoError(t, scan.ForEachField(sut, func(name string, fieldType records.FieldType) (bool, error) {
		i++
		if i > 1 {
			return true, nil
		}

		return false, nil
	}))

	assert.Equal(t, 2, i)
}

func (ts *TableScanTestSuite) TestForEachAndForeachValue() {
	t := ts.T()

	sut, tx, _, clean := ts.newSUT("")
	defer clean()
	defer func() {
		_ = tx.Commit()
	}()

	const blocks = 2
	cnt := (defaultTestBlockSize / sut.Layout().SlotSize) * blocks

	for i := 0; i < int(cnt); i++ {
		require.NoErrorf(t, sut.Insert(), "write insert i == %d", i)

		require.NoErrorf(t, sut.SetInt64("id", int64(i+1)), "write int64 i == %d", i)
		require.NoErrorf(t, sut.SetInt8("age", int8(i+2)), "write int8 i == %d", i)
		require.NoErrorf(t, sut.SetString("name", fmt.Sprintf("user %d", i)), "write string i == %d", i)
	}

	i := 0
	f := map[string]int{}

	require.NoError(t, scan.ForEach(sut, func() (bool, error) {
		require.NoError(t, scan.ForEachValue(sut, func(name string, fieldType records.FieldType, value interface{}) (bool, error) {
			switch name {
			case "id":
				assert.EqualValues(t, records.Int64Field, fieldType)
				assert.EqualValues(t, i+1, value.(int64)) //nolint:forcetypeassert
			case "name":
				assert.EqualValues(t, records.StringField, fieldType)
				assert.EqualValues(t, fmt.Sprintf("user %d", i), value.(string)) //nolint:forcetypeassert
			case "age":
				assert.EqualValues(t, records.Int8Field, fieldType)
				assert.EqualValues(t, i+2, value.(int8)) //nolint:forcetypeassert
			}

			f[name]++

			return false, nil
		}))

		i++

		return false, nil
	}))

	assert.EqualValues(t, cnt, i)
	assert.Equal(t,
		map[string]int{
			"age":  int(cnt),
			"id":   int(cnt),
			"name": int(cnt),
		},
		f,
	)
}

func (ts *TableScanTestSuite) TestForEach_Stop() {
	t := ts.T()

	sut, tx, _, clean := ts.newSUT("")
	defer clean()
	defer func() {
		_ = tx.Commit()
	}()

	const blocks = 2
	cnt := (defaultTestBlockSize / sut.Layout().SlotSize) * blocks

	for i := 0; i < int(cnt); i++ {
		require.NoErrorf(t, sut.Insert(), "write insert i == %d", i)

		require.NoErrorf(t, sut.SetInt64("id", int64(i+1)), "write int64 i == %d", i)
		require.NoErrorf(t, sut.SetInt8("age", int8(i+2)), "write int8 i == %d", i)
		require.NoErrorf(t, sut.SetString("name", fmt.Sprintf("user %d", i)), "write string i == %d", i)
	}

	i := 0

	require.NoError(t, scan.ForEach(sut, func() (bool, error) {
		if i == int(cnt/2) {
			return true, nil
		}

		i++

		return false, nil
	}))

	assert.EqualValues(t, cnt/2, i)
}

func (ts *TableScanTestSuite) TestForEachAndForEachValue_Errors() {
	t := ts.T()

	sut, tx, _, clean := ts.newSUT("")
	defer clean()
	defer func() {
		_ = tx.Commit()
	}()

	const blocks = 2
	cnt := (defaultTestBlockSize / sut.Layout().SlotSize) * blocks

	for i := 0; i < int(cnt); i++ {
		require.NoErrorf(t, sut.Insert(), "write insert i == %d", i)

		require.NoErrorf(t, sut.SetInt64("id", int64(i+1)), "write int64 i == %d", i)
		require.NoErrorf(t, sut.SetInt8("age", int8(i+2)), "write int8 i == %d", i)
		require.NoErrorf(t, sut.SetString("name", fmt.Sprintf("user %d", i)), "write string i == %d", i)
	}

	i := 0
	vErr := fmt.Errorf("foreach stop")

	require.EqualError(t, scan.ForEach(sut, func() (bool, error) {
		err := scan.ForEachValue(sut, func(name string, fieldType records.FieldType, value interface{}) (bool, error) {
			if i == int(cnt/2) {
				return false, vErr
			}

			return false, nil
		})
		if err != nil {
			require.EqualError(t, err, vErr.Error())

			return false, err
		}

		i++

		return false, nil
	}), vErr.Error())

	assert.EqualValues(t, cnt/2, i)
}
