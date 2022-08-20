package metadata

import (
	"encoding/binary"
	"sync"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/types"
)

const RefreshStatCalls = 100

type Stats struct {
	tables      *Tables
	stats       map[string]StatInfo
	fetchCounts map[string]int

	readMu sync.RWMutex
	calcMu sync.Mutex
}

func NewStats(tables *Tables, trx records.TSTRXInt) (*Stats, error) {
	s := &Stats{
		tables:      tables,
		stats:       map[string]StatInfo{},
		fetchCounts: map[string]int{},
	}

	if err := s.refreshAllTablesStats(trx); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Stats) HasStatInfo(tableName string) bool {
	s.readMu.RLock()
	defer s.readMu.RUnlock()

	_, ok := s.stats[tableName]

	return ok
}

func (s *Stats) GetStatInfo(tableName string, layout records.Layout, trx records.TSTRXInt) (StatInfo, error) {
	s.readMu.RLock()
	si, ok := s.stats[tableName]
	fetchCount := s.fetchCounts[tableName]
	s.readMu.RUnlock()

	if ok && fetchCount < RefreshStatCalls {
		s.readMu.Lock()
		s.fetchCounts[tableName]++
		s.readMu.Unlock()

		return si, nil
	}

	si, err := s.calcTableStat(tableName, trx)
	if err != nil {
		return si, err
	}

	s.readMu.Lock()
	s.stats[tableName] = si
	s.fetchCounts[tableName] = 0
	s.readMu.Unlock()

	return si, nil
}

func (s *Stats) refreshAllTablesStats(trx records.TSTRXInt) error {
	err := s.tables.ForEachTables(trx, func(tableName string) (bool, error) {
		si, err := s.calcTableStat(tableName, trx)
		if err != nil {
			return true, err
		}

		s.readMu.Lock()
		s.stats[tableName] = si
		s.fetchCounts[tableName] = 0
		s.readMu.Unlock()

		return false, nil
	})

	return err
}

func (s *Stats) calcTableStat(tableName string, trx records.TSTRXInt) (StatInfo, error) {
	s.calcMu.Lock()
	defer s.calcMu.Unlock()

	layout, err := s.tables.Layout(tableName, trx)

	switch {
	case errors.Is(err, ErrTableNotFound):
		return StatInfo{}, err
	case err != nil:
		return StatInfo{}, s.wrapError(err, tableName, nil)
	}

	si := NewStatInfo(layout.Schema)

	ts, err := records.NewTableScan(trx, tableName, layout)
	if err != nil {
		return StatInfo{}, s.wrapError(err, tableName, nil)
	}

	defer ts.Close()

	err = ts.ForEach(func() (bool, error) {
		si.Records++
		si.Blocks = int64(ts.RID().BlockNumber) + 1

		return false, ts.ForEachValue(func(name string, fieldType records.FieldType, value interface{}) (bool, error) {
			var (
				verr error
				buf  []byte
			)

			switch fieldType {
			case records.Int64Field:
				buf = make([]byte, types.Int64Size)
				binary.LittleEndian.PutUint64(buf, uint64(value.(int64))) //nolint:forcetypeassert
			case records.Int8Field:
				buf = make([]byte, 1)
				buf[0] = uint8(value.(int8)) //nolint:forcetypeassert
			case records.StringField:
				buf = []byte(value.(string)) //nolint:forcetypeassert
			default:
				verr = errors.WithMessagef(verr, "unknown field type %d for field %s", fieldType, name)
			}

			if verr != nil {
				return true, verr
			}

			si.UpdateDistincValues(name, buf)

			return false, nil
		})
	})

	if err != nil {
		return StatInfo{}, s.wrapError(err, tableName, nil)
	}

	return si, nil
}

func (s *Stats) wrapError(err error, tableName string, baseError error) error {
	if baseError == nil {
		baseError = ErrStatsMetadata
	}

	return errors.WithMessagef(baseError, "table %s: %s", tableName, err)
}
