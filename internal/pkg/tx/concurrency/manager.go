package concurrency

import "github.com/unhandled-exception/sophiadb/internal/pkg/types"

type lockType uint8

const (
	xlockType lockType = 1
	slockType lockType = 2
)

type Manager struct {
	lockTable Lockers
	locks     map[types.Block]lockType
}

var _ ConcurrencyManager = new(Manager)

func NewManager(lockTable Lockers) *Manager {
	return &Manager{
		lockTable: lockTable,
		locks:     make(map[types.Block]lockType),
	}
}

func (m *Manager) SLock(block types.Block) error {
	if _, ok := m.locks[block]; ok {
		return nil
	}

	err := m.lockTable.SLock(block)
	if err != nil {
		return err
	}

	m.locks[block] = slockType

	return nil
}

func (m *Manager) XLock(block types.Block) error {
	if m.HasXlock(block) {
		return nil
	}

	var err error

	err = m.SLock(block)
	if err != nil {
		return err
	}

	err = m.lockTable.XLock(block)
	if err != nil {
		return err
	}

	m.locks[block] = xlockType

	return nil
}

func (m *Manager) Release() {
	for block := range m.locks {
		m.lockTable.Unlock(block)
	}

	m.locks = make(map[types.Block]lockType)
}

func (m *Manager) HasXlock(block types.Block) bool {
	lock, ok := m.locks[block]

	return ok && lock == xlockType
}

func (m *Manager) HasSlock(block types.Block) bool {
	lock, ok := m.locks[block]

	return ok && lock == slockType
}
