package records_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/records"
)

type SchemaTestSuite struct {
	suite.Suite
}

func TestSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(SchemaTestSuite))
}

func (ts *SchemaTestSuite) TestCreateSchema() {
	t := ts.T()

	sut := records.NewSchema()

	sut.AddInt64Field("id")
	sut.AddStringField("username", 128)
	sut.AddStringField("job", 64)
	sut.AddInt8Field("age")

	assert.Equal(t, "id int64, username varchar(128), job varchar(64), age int8", sut.String())

	assert.Equal(t, records.Int64Field, sut.Type("id"))
	assert.Equal(t, records.StringField, sut.Type("username"))
	assert.Equal(t, records.Int8Field, sut.Type("age"))

	assert.EqualValues(t, 0, sut.Length("id"))
	assert.EqualValues(t, 128, sut.Length("username"))
	assert.EqualValues(t, 0, sut.Length("age"))

	assert.True(t, sut.HasField("id"))
	assert.False(t, sut.HasField("unexistant"))

	assert.EqualValues(t, 4, sut.Count())
}

func (ts *SchemaTestSuite) TestAddSchema() {
	t := ts.T()

	schema := records.NewSchema()
	schema.AddStringField("username", 128)
	schema.AddStringField("job", 64)
	schema.AddInt8Field("age")

	sut := records.NewSchema()
	sut.AddInt64Field("id")
	sut.AddAll(schema)

	assert.Equal(t, "id int64, username varchar(128), job varchar(64), age int8", sut.String())
}
