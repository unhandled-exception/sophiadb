package indexes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

func TestNewIndexLayout(t *testing.T) {
	type args struct {
		valueType records.FieldType
		length    int64
	}

	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "index on int64 field",
			args: args{records.Int64Field, 0},
			want: "schema: block int64, id int64, dataval int64, slot size: 25",
		},
		{
			name: "index on int8 field",
			args: args{records.Int8Field, 0},
			want: "schema: block int64, id int64, dataval int8, slot size: 18",
		},
		{
			name: "index on string field",
			args: args{records.StringField, 34},
			want: "schema: block int64, id int64, dataval varchar(34), slot size: 157",
		},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.want, indexes.NewIndexLayout(tt.args.valueType, tt.args.length).String(), tt.name)
	}
}

func TestSearchCost(t *testing.T) {
	type args struct {
		idxType         indexes.IndexType
		blocks          int64
		recordsPerBlock int64
	}

	tests := []struct {
		name string
		args args
		want int64
	}{
		{"hash index search cost", args{indexes.HashIndexType, 123456, 987654}, 1234},
		{"btree index search cost", args{indexes.BTreeIndexType, 123456, 987654}, 2},
		{"unknown index search cost", args{indexes.IndexType(-10), 123456, 987654}, -1},
	}

	for _, tt := range tests {
		assert.EqualValues(t,
			tt.want,
			indexes.SearchCost(tt.args.idxType, tt.args.blocks, tt.args.recordsPerBlock),
			tt.name,
		)
	}
}

func TestNew(t *testing.T) {
	type args struct {
		trx     scan.TRXInt
		idxType indexes.IndexType
		idxName string
		layout  records.Layout
	}

	layout := indexes.NewIndexLayout(records.Int64Field, 0)

	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{"hash index", args{nil, indexes.HashIndexType, "index1", layout}, nil},
		{"btree index", args{nil, indexes.BTreeIndexType, "index1", layout}, nil},
		{"unnown index", args{nil, indexes.IndexType(-1), "index1", layout}, indexes.ErrUnknownIndexType},
	}

	for _, tt := range tests {
		got, err := indexes.New(tt.args.trx, tt.args.idxType, tt.args.idxName, tt.args.layout)

		if tt.wantErr == nil {
			require.NoError(t, err, tt.name)

			assert.Equal(t, tt.args.idxType, got.Type())
			assert.Equal(t, tt.args.idxName, got.Name(), tt.name)
			assert.Equal(t, layout, got.Layout(), tt.name)
		} else {
			require.ErrorIs(t, err, tt.wantErr)
		}
	}
}
