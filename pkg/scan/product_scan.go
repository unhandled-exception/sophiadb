package scan

import "github.com/unhandled-exception/sophiadb/pkg/records"

type ProductScan struct {
	s1     Scan
	s2     Scan
	schema records.Schema
}

func NewProductScan(s1 Scan, s2 Scan) *ProductScan {
	s := &ProductScan{
		s1: s1,
		s2: s2,

		schema: records.NewSchema(),
	}

	s.schema.AddAll(s1.Schema())
	s.schema.AddAll(s2.Schema())

	return s
}

func (s *ProductScan) Schema() records.Schema {
	return s.schema
}

func (s *ProductScan) Close() {
	s.s1.Close()
	s.s2.Close()
}

func (s *ProductScan) BeforeFirst() error {
	if err := s.s1.BeforeFirst(); err != nil {
		return err
	}

	ok, err := s.s1.Next()
	if err != nil {
		return err
	}

	if !ok {
		return ErrEmptyScan
	}

	if err := s.s2.BeforeFirst(); err != nil {
		return err
	}

	return nil
}

func (s *ProductScan) Next() (bool, error) {
	ok, err := s.s2.Next()
	if err != nil {
		return false, err
	}

	if ok {
		return true, nil
	}

	ok, err = s.s1.Next()
	if err != nil {
		return false, err
	}

	if !ok {
		return false, nil
	}

	if err := s.s2.BeforeFirst(); err != nil {
		return false, err
	}

	return s.s2.Next()
}

func (s *ProductScan) HasField(fieldName string) bool {
	return s.schema.HasField(fieldName)
}

func (s *ProductScan) GetInt64(fieldName string) (int64, error) {
	if s.s1.Schema().HasField(fieldName) {
		return s.s1.GetInt64(fieldName)
	}

	return s.s2.GetInt64(fieldName)
}

func (s *ProductScan) GetInt8(fieldName string) (int8, error) {
	if s.s1.Schema().HasField(fieldName) {
		return s.s1.GetInt8(fieldName)
	}

	return s.s2.GetInt8(fieldName)
}

func (s *ProductScan) GetString(fieldName string) (string, error) {
	if s.s1.Schema().HasField(fieldName) {
		return s.s1.GetString(fieldName)
	}

	return s.s2.GetString(fieldName)
}

func (s *ProductScan) GetVal(fieldName string) (Constant, error) {
	if s.s1.Schema().HasField(fieldName) {
		return s.s1.GetVal(fieldName)
	}

	return s.s2.GetVal(fieldName)
}
