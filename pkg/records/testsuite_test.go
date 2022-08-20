package records_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unhandled-exception/sophiadb/pkg/buffers"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/storage"
	"github.com/unhandled-exception/sophiadb/pkg/testutil"
	"github.com/unhandled-exception/sophiadb/pkg/tx/recovery"
	"github.com/unhandled-exception/sophiadb/pkg/tx/transaction"
	"github.com/unhandled-exception/sophiadb/pkg/wal"
)

const (
	testDataFile              = "data.dat"
	testWALFile               = "record_page_wal.dat"
	defaultTestBlockSize      = 4000
	defaultTestBuffersPoolLen = 100
	defaultLockTimeout        = 100 * time.Millisecond
)

type Suite struct {
	testutil.Suite
}

func (ts *Suite) newTRXManager(lockTimeout time.Duration, path string) (*transaction.TRXManager, *storage.Manager) {
	if path == "" {
		path = ts.CreateTestTemporaryDir()
	}

	fm, err := storage.NewFileManager(path, defaultTestBlockSize)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	lm, err := wal.NewManager(fm, testWALFile)
	ts.Require().NoError(err)
	ts.Require().FileExists(filepath.Join(path, testWALFile))

	bm := buffers.NewManager(fm, lm, defaultTestBuffersPoolLen)

	m := transaction.NewTRXManager(fm, bm, lm, transaction.WithLockTimeout(lockTimeout))

	return m, fm
}

func (ts *Suite) fetchWAL(t *testing.T, trxMan *transaction.TRXManager) []string {
	it, err := trxMan.LogManager().Iterator()
	require.NoError(t, err)

	result := make([]string, 0)

	for it.HasNext() {
		raw, err := it.Next()
		require.NoError(t, err)

		lr, err := recovery.NewLogRecordFromBytes(raw)
		require.NoError(t, err)

		result = append([]string{lr.String()}, result...)
	}

	return result
}

func (ts *Suite) testLayout() records.Layout {
	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")

	return records.NewLayout(schema)
}
