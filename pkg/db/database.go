package db

import (
	"time"

	"github.com/unhandled-exception/sophiadb/internal/pkg/buffers"
	"github.com/unhandled-exception/sophiadb/internal/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/internal/pkg/planner" //nolint:typecheck
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

const (
	DefaultBlockSize   = 8 * 1024
	DefaultLogFilename = "wal_log.dat"

	DefaultBuffersPoolLen = 1024
)

var (
	DefaultPinLockTimeout         time.Duration = 1 * time.Second
	DefaultTransactionLockTimeout time.Duration = 1 * time.Second
)

type Database struct {
	blockSize      uint32
	logFileName    string
	buffersPoolLen int

	pinLockTimeout         time.Duration
	transactionLockTimeout time.Duration

	fm       *storage.Manager
	wal      *wal.Manager
	bm       *buffers.Manager
	trxMan   *transaction.TRXManager
	metadata *metadata.Manager
	planner  planner.Planner
}

type DatabaseOption func(*Database)

func NewDatabase(dataDir string, opts ...DatabaseOption) (*Database, error) {
	db := &Database{
		blockSize:      DefaultBlockSize,
		logFileName:    DefaultLogFilename,
		buffersPoolLen: DefaultBuffersPoolLen,

		pinLockTimeout:         DefaultPinLockTimeout,
		transactionLockTimeout: DefaultTransactionLockTimeout,
	}

	for _, opt := range opts {
		opt(db)
	}

	fm, err := storage.NewFileManager(dataDir, db.blockSize)
	if err != nil {
		return nil, err
	}

	wal, err := wal.NewManager(fm, db.logFileName)
	if err != nil {
		return nil, err
	}

	db.fm = fm
	db.wal = wal

	db.bm = buffers.NewManager(db.fm, db.wal, db.buffersPoolLen, buffers.WithPinLockTimeout(db.pinLockTimeout))
	db.trxMan = transaction.NewTRXManager(db.fm, db.bm, db.wal, transaction.WithLockTimeout(db.transactionLockTimeout))

	db.metadata, err = db.newMetadataManager()
	if err != nil {
		return nil, err
	}

	db.planner = planner.NewSQLPlanner(
		planner.NewSQLQueryPlanner(db.metadata),
		planner.NewSQLCommandsPlanner(db.metadata),
	)

	return db, nil
}

func WithBlockSize(blockSize uint32) DatabaseOption {
	return func(db *Database) {
		db.blockSize = blockSize
	}
}

func WithLogFileName(logFileName string) DatabaseOption {
	return func(db *Database) {
		db.logFileName = logFileName
	}
}

func WithBuffersPoolLen(buffersPoolLen int) DatabaseOption {
	return func(db *Database) {
		db.buffersPoolLen = buffersPoolLen
	}
}

func WithPinLockTimeout(pinLockTimeout time.Duration) DatabaseOption {
	return func(db *Database) {
		db.pinLockTimeout = pinLockTimeout
	}
}

func WithTransactionLockTimeout(transactionLockTimeout time.Duration) DatabaseOption {
	return func(db *Database) {
		db.transactionLockTimeout = transactionLockTimeout
	}
}

func (db *Database) Planner() planner.Planner {
	return db.planner
}

func (db *Database) Close() error {
	return db.fm.Close()
}

func (db *Database) Transaction() (*transaction.Transaction, error) {
	return db.trxMan.Transaction()
}

func (db *Database) IsNew() bool {
	return db.fm.IsNew
}

func (db *Database) DataDir() string {
	return db.fm.Path()
}

func (db *Database) BlockSize() uint32 {
	return db.fm.BlockSize()
}

func (db *Database) LogFileName() string {
	return db.wal.LogFileName
}

func (db *Database) BuffersPoolLen() int {
	return db.bm.Len
}

func (db *Database) PinLockTimeout() time.Duration {
	return db.bm.PinLockTimeout
}

func (db *Database) TransactionLockTimeout() time.Duration {
	return db.trxMan.LockTimeout
}

func (db *Database) newMetadataManager() (*metadata.Manager, error) {
	var err error

	trx, err := db.trxMan.Transaction()
	if err != nil {
		return nil, err
	}

	isNew := db.fm.IsNew

	if !isNew {
		if err = trx.Recover(); err != nil {
			return nil, err
		}
	}

	mdm, err := metadata.NewManager(isNew, trx)
	if err != nil {
		return nil, err
	}

	if err := trx.Commit(); err != nil {
		return nil, err
	}

	return mdm, nil
}
