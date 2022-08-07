package metadata_test

import (
	"path/filepath"
	"time"

	"github.com/unhandled-exception/sophiadb/internal/pkg/buffers"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/testutil"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

const (
	testWALFile               = "record_page_wal.dat"
	defaultTestBlockSize      = 4000
	defaultTestBuffersPoolLen = 100
	defaultLockTimeout        = 100 * time.Millisecond
)

type Suite struct {
	testutil.Suite
}

func (ts *Suite) newTRXManager(lockTimeout time.Duration, path string) (*transaction.TRXManager, func()) {
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

	return m, func() {
		_ = fm.Close()
	}
}
