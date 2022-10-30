package transaction

import (
	"time"

	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/concurrency"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type TRXManager struct {
	fm storageManager
	bm buffersManager
	lm logManager

	LockTimeout time.Duration
	lockTable   concurrency.Lockers
	trxGen      *TRXGenerator
}

type trxManagerOpt func(*TRXManager)

func NewTRXManager(fm storageManager, bm buffersManager, lm logManager, opts ...trxManagerOpt) *TRXManager {
	m := &TRXManager{
		fm:     fm,
		bm:     bm,
		lm:     lm,
		trxGen: NewTRXGenerator(),
	}

	for _, opt := range opts {
		opt(m)
	}

	m.lockTable = concurrency.NewLockTable(
		concurrency.WithLockWaitTimeout(m.LockTimeout),
	)

	return m
}

func WithLockTimeout(timoout time.Duration) trxManagerOpt {
	return func(m *TRXManager) {
		m.LockTimeout = timoout
	}
}

func (m *TRXManager) Transaction() (*Transaction, error) {
	return NewTransaction(m.trxGen.NextTRX, m.fm, m.lm, m.bm, m.lockTable)
}

func (m *TRXManager) TRXGen() *TRXGenerator {
	return m.trxGen
}

func (m *TRXManager) SetLastTRX(lastTRX types.TRX) {
	m.trxGen.SetLastTRX(lastTRX)
}

func (m *TRXManager) LogManager() logManager {
	return m.lm
}
