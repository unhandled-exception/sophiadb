package scan

import "github.com/unhandled-exception/sophiadb/internal/pkg/records"

type ProjectScan struct {
	s      Scan
	schema records.Schema
}

func NewProjectScan(s Scan, fields ...string) *ProjectScan {
	ps := &ProjectScan{
		s:      s,
		schema: records.NewSchema(),
	}

	parentSchema := s.Schema()

	for _, field := range fields {
		fi, ok := parentSchema.Field(field)
		if !ok {
			continue
		}

		ps.schema.AddField(field, fi.Type, fi.Length)
	}

	return ps
}

func (s *ProjectScan) Schema() records.Schema {
	return s.schema
}

func (s *ProjectScan) Close() {
	s.s.Close()
}

func (s *ProjectScan) BeforeFirst() error {
	return s.s.BeforeFirst()
}

func (s *ProjectScan) Next() (bool, error) {
	return s.s.Next()
}

func (s *ProjectScan) HasField(fieldName string) bool {
	return s.schema.HasField(fieldName)
}

func (s *ProjectScan) GetInt64(fieldName string) (int64, error) {
	if !s.HasField(fieldName) {
		return 0, ErrFieldNotFound
	}

	return s.s.GetInt64(fieldName)
}

func (s *ProjectScan) GetInt8(fieldName string) (int8, error) {
	if !s.HasField(fieldName) {
		return 0, ErrFieldNotFound
	}

	return s.s.GetInt8(fieldName)
}

func (s *ProjectScan) GetString(fieldName string) (string, error) {
	if !s.HasField(fieldName) {
		return "", ErrFieldNotFound
	}

	return s.s.GetString(fieldName)
}

func (s *ProjectScan) GetVal(fieldName string) (Constant, error) {
	if !s.HasField(fieldName) {
		return nil, ErrFieldNotFound
	}

	return s.s.GetVal(fieldName)
}
