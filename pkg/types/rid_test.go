package types_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/unhandled-exception/sophiadb/pkg/types"
)

func TestRID(t *testing.T) {
	rid1 := types.RID{
		BlockNumber: 123,
		Slot:        156,
	}
	rid2 := types.RID{
		BlockNumber: 223,
		Slot:        256,
	}

	assert.EqualValues(t, "RID[123, 156]", rid1.String())

	rid3 := rid1
	assert.False(t, rid1.Equals(rid2))
	assert.True(t, rid1.Equals(rid3))

	rid4 := types.RID{}
	assert.True(t, types.RID{}.Equals(rid4))
}
