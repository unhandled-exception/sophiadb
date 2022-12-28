package indexplanner

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type JoinScan struct {
	lhs   scan.Scan
	rIdx  indexes.Index
	field string
	rhs   *scan.TableScan
}

func NewJoinScan(lhs scan.Scan, rIdx indexes.Index, field string, rhs *scan.TableScan) (*JoinScan, error) {
	s := &JoinScan{
		lhs:   lhs,
		rIdx:  rIdx,
		field: field,
		rhs:   rhs,
	}

	return s, nil
}

func (s *JoinScan) Schema() records.Schema {
	panic("not implemented") // TODO: Implement
}

func (s *JoinScan) Close() {
	panic("not implemented") // TODO: Implement
}

func (s *JoinScan) BeforeFirst() error {
	panic("not implemented") // TODO: Implement
}

func (s *JoinScan) Next() (bool, error) {
	panic("not implemented") // TODO: Implement
}

func (s *JoinScan) HasField(fieldName string) bool {
	panic("not implemented") // TODO: Implement
}

func (s *JoinScan) GetInt64(fieldName string) (int64, error) {
	panic("not implemented") // TODO: Implement
}

func (s *JoinScan) GetInt8(fieldName string) (int8, error) {
	panic("not implemented") // TODO: Implement
}

func (s *JoinScan) GetString(fieldName string) (string, error) {
	panic("not implemented") // TODO: Implement
}

func (s *JoinScan) GetVal(fieldName string) (scan.Constant, error) {
	panic("not implemented") // TODO: Implement
}
