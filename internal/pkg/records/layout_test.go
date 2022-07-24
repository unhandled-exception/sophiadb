package records_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
)

type LayoutTestSuite struct {
	suite.Suite
}

func TestLayoutTestSuite(t *testing.T) {
	suite.Run(t, new(LayoutTestSuite))
}

func (ts *LayoutTestSuite) TestCreateLayout() {
	t := ts.T()

	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("username", 128)
	schema.AddStringField("job", 64)
	schema.AddInt64Field("age")

	sut := records.NewLayout(schema)

	assert.EqualValues(t, 1+8+(128*4+4)+(64*4+4)+8, sut.SlotSize)
	assert.EqualValues(t, 1+8+(128*4+4), sut.Offset("job"))
}
