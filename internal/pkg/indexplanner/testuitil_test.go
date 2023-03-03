package indexplanner_test

import (
	"path/filepath"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unhandled-exception/sophiadb/internal/pkg/buffers"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/testutil"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

const (
	testDataTable             = "data"
	testWALFile               = "indexplanner_wal.dat"
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

func (ts *Suite) testLayout() records.Layout {
	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")
	schema.AddInt64Field("_hidden")

	return records.NewLayout(schema)
}

func (ts *Suite) testLayout2() records.Layout {
	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("job", 45)

	return records.NewLayout(schema)
}

func (ts *Suite) requireRowsCount(expected int, sc scan.Scan, msgAndArgs ...any) {
	t := ts.T()

	resCnt := 0

	require.NoError(t, scan.ForEach(sc, func() (stop bool, err error) {
		resCnt++

		return false, nil
	}), msgAndArgs...)

	require.EqualValues(t, expected, resCnt, msgAndArgs...)
}
