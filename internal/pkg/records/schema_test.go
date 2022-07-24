package records_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
)

type SchemaTestSuite struct {
	suite.Suite
}

func TestSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(SchemaTestSuite))
}

func (ts SchemaTestSuite) TestCreateSchema() {
	t := ts.T()

	sut := records.NewSchema()

	sut.AddInt64Field("id")
	sut.AddStringField("username", 128)
	sut.AddStringField("job", 64)
	sut.AddInt64Field("age")

	assert.Equal(t, "[id: int64], [username: string(128)], [job: string(64)], [age: int64]", sut.String())

	assert.Equal(t, records.Int64Field, sut.Type("id"))
	assert.Equal(t, records.StringField, sut.Type("username"))

	assert.Equal(t, 0, sut.Length("id"))
	assert.Equal(t, 128, sut.Length("username"))

	assert.True(t, sut.HasField("id"))
	assert.False(t, sut.HasField("unexistant"))
}

func (ts SchemaTestSuite) TestAddSchema() {
	t := ts.T()

	schema := records.NewSchema()
	schema.AddStringField("username", 128)
	schema.AddStringField("job", 64)
	schema.AddInt64Field("age")

	sut := records.NewSchema()
	sut.AddInt64Field("id")
	sut.AddAll(schema)

	assert.Equal(t, "[id: int64], [username: string(128)], [job: string(64)], [age: int64]", sut.String())
}
