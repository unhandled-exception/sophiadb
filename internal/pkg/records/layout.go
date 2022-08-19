package records

import (
	"fmt"

	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type Layout struct {
	Schema   Schema
	SlotSize uint32
	Offsets  map[string]uint32
}

func NewLayout(schema Schema) Layout {
	l := Layout{
		Schema:  schema,
		Offsets: make(map[string]uint32, schema.Count()),
	}

	size := uint32(types.Int8Size)
	for _, name := range schema.Fields() {
		l.Offsets[name] = size

		field, _ := schema.Field(name)
		size += field.BytesLen()
	}

	l.SlotSize = size

	return l
}

func (l Layout) String() string {
	return fmt.Sprintf(
		"schema: %s, slot size: %d",
		l.Schema,
		l.SlotSize,
	)
}

func (l Layout) Offset(name string) uint32 {
	return l.Offsets[name]
}
