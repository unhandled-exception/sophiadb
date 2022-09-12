package scan

import "github.com/unhandled-exception/sophiadb/pkg/records"

type ProjectScan struct {
	s      Scan
	fields []string
}

func NewProjectScan(s Scan, fields ...string) *ProjectScan {
	ps := &ProjectScan{
		s:      s,
		fields: append(make([]string, 0, len(fields)), fields...),
	}

	return ps
}

func (s *ProjectScan) Schema() records.Schema {
	panic("not implemented") // TODO: Implement
}

func (s *ProjectScan) Close() {
	panic("not implemented") // TODO: Implement
}

func (s *ProjectScan) BeforeFirst() error {
	panic("not implemented") // TODO: Implement
}

func (s *ProjectScan) Next() (bool, error) {
	panic("not implemented") // TODO: Implement
}

func (s *ProjectScan) HasField(fieldName string) bool {
	panic("not implemented") // TODO: Implement
}

func (s *ProjectScan) GetInt64(fieldName string) (int64, error) {
	panic("not implemented") // TODO: Implement
}

func (s *ProjectScan) GetInt8(fieldName string) (int8, error) {
	panic("not implemented") // TODO: Implement
}

func (s *ProjectScan) GetString(fieldName string) (string, error) {
	panic("not implemented") // TODO: Implement
}

func (s *ProjectScan) GetVal(fieldName string) (Constant, error) {
	panic("not implemented") // TODO: Implement
}
