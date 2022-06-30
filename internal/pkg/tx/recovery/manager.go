package recovery

type Manager struct {
	trx trxInt
	bm  bufferManager
	lm  walManager
}

func NewManager(trx trxInt, lm walManager, bm bufferManager) *Manager {
	return &Manager{
		trx: trx,
		bm:  bm,
		lm:  lm,
	}
}

func (m *Manager) Commit() error {
	return nil
}

func (m *Manager) Rollback() error {
	return nil
}

func (m *Manager) Recover() error {
	return nil
}

func (m *Manager) SetInt64() (int32, error) {
	return -1, nil
}
