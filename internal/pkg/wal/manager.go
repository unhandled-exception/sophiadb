package wal

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

const (
	int32Size  = 4
	blockStart = 0
)

// Manager — диспетчер журнала
type Manager struct {
	m sync.Mutex

	fm           *storage.Manager
	logFileName  string
	logPage      *types.Page
	currentBlock types.Block
	latestLSN    types.LSN
	lastSavedLSN types.LSN
}

// NewManager создает новый объект LogManager
func NewManager(fm *storage.Manager, logFileName string) (*Manager, error) {
	lm := &Manager{
		fm:          fm,
		logFileName: logFileName,
		logPage:     types.NewPage(fm.BlockSize()),
	}

	logSize, err := fm.Length(logFileName)
	if err != nil {
		return nil, errors.WithMessage(ErrFailedToCreateNewManager, err.Error())
	}

	if logSize == 0 {
		lm.currentBlock, err = lm.appendNewBlock()
		if err != nil {
			return nil, errors.WithMessage(ErrFailedToCreateNewManager, err.Error())
		}
	} else {
		lm.currentBlock = types.NewBlock(lm.logFileName, logSize-1)
		err = lm.fm.Read(lm.currentBlock, lm.logPage)
		if err != nil {
			return nil, errors.WithMessage(ErrFailedToCreateNewManager, err.Error())
		}
	}

	return lm, nil
}

// StorageManager возвращает менеджер хранилища
func (lm *Manager) StorageManager() *storage.Manager {
	return lm.fm
}

// CurrentBlock возвращает текущий блок
func (lm *Manager) CurrentBlock() types.Block {
	return lm.currentBlock
}

// Flush сбрасывает журнал на диск
func (lm *Manager) Flush(lsn types.LSN, force bool) error {
	return lm.flush(lsn, force, false)
}

func (lm *Manager) flush(lsn types.LSN, force bool, skipLock bool) error {
	if !skipLock {
		lm.m.Lock()
		defer lm.m.Unlock()
	}

	if lsn >= lm.lastSavedLSN || force {
		err := lm.fm.Write(lm.currentBlock, lm.logPage)
		if err != nil {
			return err
		}
	}

	return nil
}

// Iterator возвращает новый итератор по журналу
func (lm *Manager) Iterator() (*Iterator, error) {
	if err := lm.Flush(0, true); err != nil {
		return nil, err
	}

	it, err := NewIterator(lm.fm, lm.currentBlock)
	if err != nil {
		return nil, err
	}

	return it, nil
}

// Append добавляет в журнал новую запись
func (lm *Manager) Append(logRec []byte) (types.LSN, error) {
	lm.m.Lock()
	defer lm.m.Unlock()

	boundary := lm.logPage.GetUint32(blockStart)
	recsize := uint32(len(logRec))
	bytesNeeded := recsize + int32Size

	if int(boundary)-int(bytesNeeded) < int32Size {
		// Если данные не умещаются в блок, то:
		// — cбрасываем текущий блок на диск
		// — создаем новый блок
		err := lm.flush(0, true, true)
		if err != nil {
			return 0, errors.WithMessage(ErrFailedToAppendNewRecord, err.Error())
		}

		lm.currentBlock, err = lm.appendNewBlock()
		if err != nil {
			return 0, errors.WithMessage(ErrFailedToAppendNewRecord, err.Error())
		}

		boundary = lm.logPage.GetUint32(blockStart)
	}

	// Новую запись пишем в конец блока. Конец — это граница последней записи в логе
	recPos := boundary - bytesNeeded
	lm.logPage.SetBytes(recPos, logRec)
	lm.logPage.SetUint32(blockStart, recPos) // Устанавливаем новую границу

	lm.latestLSN++

	return lm.latestLSN, nil
}

// appendNewBlock добавляет новый блок в журнал
func (lm *Manager) appendNewBlock() (types.Block, error) {
	blk, err := lm.fm.Append(lm.logFileName)
	if err != nil {
		return blk, err
	}

	lm.logPage.SetUint32(blockStart, lm.fm.BlockSize())

	if err = lm.fm.Write(blk, lm.logPage); err != nil {
		return blk, err
	}

	return blk, nil
}
