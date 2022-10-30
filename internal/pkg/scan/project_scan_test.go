package scan_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type ProjectScanTestsuite struct {
	Suite
}

var _ scan.Scan = &scan.ProjectScan{}

func TestProjectScanTestsuite(t *testing.T) {
	suite.Run(t, new(ProjectScanTestsuite))
}

func (ts *ProjectScanTestsuite) testLayout() records.Layout {
	schema := ts.Suite.testLayout().Schema

	schema.AddField("job", records.StringField, 20)
	schema.AddField("room", records.Int64Field, 0)
	schema.AddField("place", records.Int8Field, 0)
	schema.AddField("_invisible", records.Int64Field, 0)

	return records.NewLayout(schema)
}

func (ts *ProjectScanTestsuite) TestSchema() {
	t := ts.T()

	tm, sm := ts.newTRXManager(defaultLockTimeout, "")
	defer sm.Close()

	tx, err := tm.Transaction()
	require.NoError(t, err)

	defer require.NoError(t, tx.Commit())

	ts1, err := scan.NewTableScan(tx, testDataTable, ts.testLayout())
	require.NoError(t, err)

	sut := scan.NewProjectScan(ts1, "id", "name", "unexistant")

	defer sut.Close()

	assert.Equal(t, "id int64, name varchar(25)", sut.Schema().String())
}

func (ts *ProjectScanTestsuite) TestIterate() {
	t := ts.T()

	tm, sm := ts.newTRXManager(defaultLockTimeout, "")
	defer sm.Close()

	tx, err := tm.Transaction()
	require.NoError(t, err)

	ts1, err := scan.NewTableScan(tx, testDataTable, ts.testLayout())
	require.NoError(t, err)

	records := 1000

	for i := 0; i < records; i++ {
		require.NoError(t, ts1.Insert())
		require.NoError(t, ts1.SetInt64("id", int64(i)))
		require.NoError(t, ts1.SetString("name", fmt.Sprintf("user %d", i)))
		require.NoError(t, ts1.SetInt8("age", int8(i%128)))
		require.NoError(t, ts1.SetInt64("_hidden", int64(i/2)))

		require.NoError(t, ts1.SetString("job", fmt.Sprintf("job %d", i)))
		require.NoError(t, ts1.SetInt64("room", -1*int64(i)))
		require.NoError(t, ts1.SetInt8("place", -1*int8(i%128)))
		require.NoError(t, ts1.SetVal("_invisible", scan.NewInt64Constant(-1*int64(i/2))))
	}

	require.NoError(t, tx.Commit())

	ts2, err := scan.NewTableScan(tx, testDataTable, ts.testLayout())
	require.NoError(t, err)

	defer require.NoError(t, tx.Commit())

	sut := scan.NewProjectScan(ts2, "id", "name", "age", "_hidden")

	var i int64 = 0

	require.NoError(t, scan.ForEach(sut, func() (bool, error) {
		_, err := sut.GetString("job")
		require.ErrorIs(t, err, scan.ErrFieldNotFound)

		_, err = sut.GetInt64("room")
		require.ErrorIs(t, err, scan.ErrFieldNotFound)

		_, err = sut.GetInt8("place")
		require.ErrorIs(t, err, scan.ErrFieldNotFound)

		_, err = sut.GetVal("_invisible")
		require.ErrorIs(t, err, scan.ErrFieldNotFound)

		id, err := sut.GetInt64("id")
		require.NoError(t, err)
		assert.EqualValues(t, i, id)

		age, err := sut.GetInt8("age")
		require.NoError(t, err)
		assert.EqualValues(t, i%128, age)

		name, err := sut.GetString("name")
		require.NoError(t, err)
		assert.EqualValues(t, fmt.Sprintf("user %d", i), name)

		hidden, err := sut.GetVal("_hidden")
		require.NoError(t, err)
		assert.Equal(t, scan.CompEqual, scan.NewInt64Constant(i/2).CompareTo(hidden))

		i++

		return false, nil
	}))

	assert.EqualValues(t, records, i)

	defer sut.Close()
}
