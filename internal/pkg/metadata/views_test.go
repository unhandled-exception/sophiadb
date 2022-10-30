package metadata_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
)

type ViewsTestSuite struct {
	Suite
}

func TestViewsTestSuite(t *testing.T) {
	suite.Run(t, new(ViewsTestSuite))
}

func (ts *ViewsTestSuite) newSUT(t *testing.T) (*metadata.Views, *transaction.Transaction, func()) {
	trxMan, clean := ts.newTRXManager(defaultLockTimeout, t.TempDir())
	defer clean()

	strx, err := trxMan.Transaction()
	require.NoError(t, err)

	tables, err := metadata.NewTables(true, strx)
	require.NoError(t, err)

	sut, err := metadata.NewViews(tables, true, strx)
	require.NoError(t, err)

	require.NoError(t, strx.Commit())

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	return sut, trx, func() {
		clean()
	}
}

func (ts *ViewsTestSuite) TestCreateView_Ok() {
	t := ts.T()

	sut, trx, clean := ts.newSUT(t)
	defer clean()

	for i := 0; i < 100; i++ {
		require.NoError(t, sut.CreateView(
			fmt.Sprintf("view_%d", i),
			fmt.Sprintf("create view %d", i),
			trx,
		))
	}

	recs, err := scan.NewTableScan(trx, sut.VcatTableName, sut.VcatLayout)
	require.NoError(t, err)

	i := 0

	require.NoError(t, scan.ForEach(recs, func() (bool, error) {
		vn, err := recs.GetString(metadata.VcatViewNameField)
		require.NoError(t, err)

		vd, err := recs.GetString(metadata.VcatViewDefField)
		require.NoError(t, err)

		assert.Equal(t, fmt.Sprintf("view_%d", i), vn)
		assert.Equal(t, fmt.Sprintf("create view %d", i), vd)

		i++

		return false, nil
	}))
}

func (ts *ViewsTestSuite) TestCreateView_ViewExists() {
	t := ts.T()

	sut, trx, clean := ts.newSUT(t)
	defer clean()

	require.NoError(t, sut.CreateView("test_view", "create test view", trx))

	assert.ErrorIs(t, sut.CreateView("test_view", "create test view", trx), metadata.ErrViewExists)
}

func (ts *ViewsTestSuite) TestViewDef_Ok() {
	t := ts.T()

	sut, trx, clean := ts.newSUT(t)
	defer clean()

	for i := 0; i < 100; i++ {
		require.NoError(t, sut.CreateView(
			fmt.Sprintf("view_%d", i),
			fmt.Sprintf("create view %d", i),
			trx,
		))
	}

	for i := 0; i < 100; i++ {
		vd, err := sut.ViewDef(fmt.Sprintf("view_%d", i), trx)
		assert.NoError(t, err)
		assert.Equal(t, vd, fmt.Sprintf("create view %d", i))
	}
}

func (ts *ViewsTestSuite) TestViewDef_NoViewFound() {
	t := ts.T()

	sut, trx, clean := ts.newSUT(t)
	defer clean()

	require.NoError(t, sut.CreateView("test_view", "create test view", trx))

	_, err := sut.ViewDef("unexistant_view", trx)
	assert.ErrorIs(t, err, metadata.ErrViewNotFound)
}

func (ts *ViewsTestSuite) TestViewExists() {
	t := ts.T()

	sut, trx, clean := ts.newSUT(t)
	defer clean()

	exists, err := sut.ViewExists("unexistant_view", trx)
	require.NoError(t, err)
	assert.False(t, exists)

	for i := 0; i < 100; i++ {
		require.NoError(t, sut.CreateView(
			fmt.Sprintf("view_%d", i),
			fmt.Sprintf("create view %d", i),
			trx,
		))
		require.NoError(t, err)
	}

	exists, err = sut.ViewExists("view_90", trx)
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = sut.ViewExists("unexistant_view", trx)
	require.NoError(t, err)
	assert.False(t, exists)
}
