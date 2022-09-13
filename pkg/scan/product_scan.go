package scan

import "github.com/unhandled-exception/sophiadb/pkg/records"

type ProductScan struct{}

func NewproductScan(s1 Scan, s2 Scan) *ProductScan {
	ps := &ProductScan{}

	return ps
}

func (s *ProductScan) Schema() records.Schema {
	panic("not implemented") // TODO: Implement
}

func (s *ProductScan) Close() {
	panic("not implemented") // TODO: Implement
}

func (s *ProductScan) BeforeFirst() error {
	panic("not implemented") // TODO: Implement
}

func (s *ProductScan) Next() (bool, error) {
	panic("not implemented") // TODO: Implement
}

func (s *ProductScan) HasField(fieldName string) bool {
	panic("not implemented") // TODO: Implement
}

func (s *ProductScan) GetInt64(fieldName string) (int64, error) {
	panic("not implemented") // TODO: Implement
}

func (s *ProductScan) GetInt8(fieldName string) (int8, error) {
	panic("not implemented") // TODO: Implement
}

func (s *ProductScan) GetString(fieldName string) (string, error) {
	panic("not implemented") // TODO: Implement
}

func (s *ProductScan) GetVal(fieldName string) (Constant, error) {
	panic("not implemented") // TODO: Implement
}
