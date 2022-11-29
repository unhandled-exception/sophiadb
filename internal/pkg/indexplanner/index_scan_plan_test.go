package indexplanner_test

import (
	"fmt"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexplanner"
	"github.com/unhandled-exception/sophiadb/internal/pkg/planner"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

var _ planner.Plan = &indexplanner.IndexPlan{}

type IndexScanPlanTestSuite struct {
	Suite
}

func TestIndexScanPlanTestSuite(t *testing.T) {
	suite.Run(t, new(IndexScanPlanTestSuite))
}

func (ts *IndexScanPlanTestSuite) TestScanPlan_Ok() {
	t := ts.T()
	mc := minimock.NewController(t)

	schema := ts.testLayout().Schema

	tp := indexplanner.NewPlanMock(mc).
		SchemaMock.Return(schema).
		OpenMock.Return(&scan.TableScan{}, nil)

	idx := indexplanner.NewIndexInfoMock(mc).
		BlocksAccessedMock.Return(123).
		RecordsMock.Return(894).
		DistinctValuesMock.Return(6543).
		StringMock.Return("table1.idx1").
		OpenMock.Return(&indexes.BaseIndex{}, nil)

	value := scan.NewInt64Constant(12345)

	sut, err := indexplanner.NewIndexPlan(tp, idx, value)
	require.NoError(t, err)

	assert.Equal(t, "index scan on \"table1.idx1\"", sut.String())

	assert.Equal(t, schema, sut.Schema())
	assert.EqualValues(t, 123+894, sut.BlocksAccessed())
	assert.EqualValues(t, 894, sut.Records())

	dv, ok := sut.DistinctValues("id")
	assert.EqualValues(t, 6543, dv)
	assert.True(t, ok)

	is, err := sut.Open()
	require.NoError(t, err)
	assert.NotNil(t, is)
}

func (ts *IndexScanPlanTestSuite) TestScanPlan_FailedToOpenWrappedPlan() {
	t := ts.T()
	mc := minimock.NewController(t)

	tp := indexplanner.NewPlanMock(mc).
		OpenMock.Return(nil, fmt.Errorf("failed to open plan"))

	idx := indexplanner.NewIndexInfoMock(mc)

	value := scan.NewInt64Constant(12345)

	sut, err := indexplanner.NewIndexPlan(tp, idx, value)
	require.NoError(t, err)

	_, err = sut.Open()
	assert.ErrorIs(t, err, planner.ErrFailedToCreatePlan)
}

func (ts *IndexScanPlanTestSuite) TestScanPlan_InvalidWrappedPlan() {
	t := ts.T()
	mc := minimock.NewController(t)

	tp := indexplanner.NewPlanMock(mc).
		OpenMock.Return(&scan.SelectScan{}, nil)

	idx := indexplanner.NewIndexInfoMock(mc)

	value := scan.NewInt64Constant(12345)

	sut, err := indexplanner.NewIndexPlan(tp, idx, value)
	require.NoError(t, err)

	_, err = sut.Open()
	assert.ErrorIs(t, err, planner.ErrFailedToCreatePlan)
}

func (ts *IndexScanPlanTestSuite) TestScanPlan_FailedToOpenIndex() {
	t := ts.T()
	mc := minimock.NewController(t)

	tp := indexplanner.NewPlanMock(mc).
		OpenMock.Return(&scan.TableScan{}, nil)

	idx := indexplanner.NewIndexInfoMock(mc).
		OpenMock.Return(nil, fmt.Errorf("failed to open index"))

	value := scan.NewInt64Constant(12345)

	sut, err := indexplanner.NewIndexPlan(tp, idx, value)
	require.NoError(t, err)

	_, err = sut.Open()
	assert.ErrorIs(t, err, planner.ErrFailedToCreatePlan)
}
