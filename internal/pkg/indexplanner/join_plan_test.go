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

var _ planner.Plan = &indexplanner.JoinPlan{}

type JoinPlanTestSute struct {
	Suite
}

func TestJoinPlanTestSute(t *testing.T) {
	suite.Run(t, new(JoinPlanTestSute))
}

func (ts *JoinPlanTestSute) TestCreatePlan_Ok() {
	t := ts.T()
	mc := minimock.NewController(t)

	p1 := indexplanner.NewJPlanMock(mc).
		SchemaMock.Return(ts.testLayout().Schema).
		OpenMock.Return(&scan.TableScan{}, nil).
		StringMock.Return("<p1 plan>").
		BlocksAccessedMock.Return(65).
		RecordsMock.Return(321).
		DistinctValuesMock.Return(100, false)

	p2 := indexplanner.NewJPlanMock(mc).
		SchemaMock.Return(ts.testLayout2().Schema).
		OpenMock.Return(&scan.TableScan{}, nil).
		StringMock.Return("<p2 plan>").
		DistinctValuesMock.Return(50, true)

	idx := indexplanner.NewJIndexInfoMock(mc).
		BlocksAccessedMock.Return(123).
		RecordsMock.Return(894).
		OpenMock.Return(&indexes.BaseIndex{}, nil).
		StringMock.Return("table1.idx1").
		BlocksAccessedMock.Return(75)

	sut, err := indexplanner.NewJoinPlan(p1, p2, idx, "id")
	require.NoError(t, err)

	assert.Equal(t, `join (<p1 plan>) to (<p2 plan>) on index ("table1.idx1")`, sut.String())
	assert.Equal(t, `id int64, name varchar(25), age int8, _hidden int64, id int64, job varchar(45)`, sut.Schema().String())

	assert.EqualValues(t, 311114, sut.BlocksAccessed())
	assert.EqualValues(t, 286974, sut.Records())

	js, err := sut.Open()
	require.NoError(t, err)
	assert.NotNil(t, js)

	dv, ok := sut.DistinctValues("id")
	assert.EqualValues(t, 50, dv)
	assert.True(t, ok)

	p1.DistinctValuesMock.Return(101, true)
	dv, ok = sut.DistinctValues("id")
	assert.EqualValues(t, 101, dv)
	assert.True(t, ok)
}

func (ts *JoinPlanTestSute) TestCreatePlan_Errors() {
	t := ts.T()
	mc := minimock.NewController(t)

	p1 := indexplanner.NewJPlanMock(mc).
		SchemaMock.Return(ts.testLayout().Schema).
		OpenMock.Return(&scan.TableScan{}, nil)

	p2 := indexplanner.NewJPlanMock(mc).
		SchemaMock.Return(ts.testLayout2().Schema).
		OpenMock.Return(&scan.TableScan{}, nil)

	idx := indexplanner.NewJIndexInfoMock(mc)

	// p1 не открывается
	p1.OpenMock.Return(nil, fmt.Errorf("failed to open p1"))

	sut, err := indexplanner.NewJoinPlan(p1, p2, idx, "id")
	require.NoError(t, err)

	_, err = sut.Open()
	assert.ErrorIs(t, err, planner.ErrFailedToCreatePlan)

	// p2 не открывается
	p1.OpenMock.Return(&scan.TableScan{}, nil)
	p2.OpenMock.Return(nil, fmt.Errorf("failed to open p2"))

	sut, err = indexplanner.NewJoinPlan(p1, p2, idx, "id")
	require.NoError(t, err)

	_, err = sut.Open()
	assert.ErrorIs(t, err, planner.ErrFailedToCreatePlan)

	// p2 не TableScan
	p2.OpenMock.Return(nil, nil)

	sut, err = indexplanner.NewJoinPlan(p1, p2, idx, "id")
	require.NoError(t, err)

	_, err = sut.Open()
	assert.ErrorIs(t, err, planner.ErrFailedToCreatePlan)

	// Индекс не открывается
	p1.OpenMock.Return(&scan.TableScan{}, nil)
	p2.OpenMock.Return(&scan.TableScan{}, nil)
	idx.OpenMock.Return(nil, fmt.Errorf("failed to open index"))

	sut, err = indexplanner.NewJoinPlan(p1, p2, idx, "id")
	require.NoError(t, err)

	_, err = sut.Open()
	assert.ErrorIs(t, err, planner.ErrFailedToCreatePlan)
}
