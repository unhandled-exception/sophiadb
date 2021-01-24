package wal

import (
	"sync"

	"github.com/rotisserie/eris"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
)

const (
	int32Size  = 4
	blockStart = 0
)

// Manager — диспетчер журнала
type Manager struct {
	sync.Mutex

	fm           *storage.FileManager
	logFileName  string
	logPage      *storage.Page
	currentBlock *storage.BlockID
	latestLSN    uint32
	lastSavedLSN uint32
}

// ErrFailedToCreateNewManager — ошибка при создании нового менеджера
var ErrFailedToCreateNewManager = eris.New("failed to create a new wal manager")

// ErrFailedToAppendNewRecord — ошибка при создании новой записи в wal-логе
var ErrFailedToAppendNewRecord = eris.New("failed to add new record to wal log")

// NewManager создает новый объект LogManager
func NewManager(fm *storage.FileManager, logFileName string) (*Manager, error) {
	lm := &Manager{
		fm:          fm,
		logFileName: logFileName,
		logPage:     storage.NewPage(fm.BlockSize()),
	}
	logSize, err := fm.Length(logFileName)
	if err != nil {
		return nil, eris.Wrap(err, ErrFailedToCreateNewManager.Error())
	}

	if logSize == 0 {
		lm.currentBlock, err = lm.appendNewBlock()
		if err != nil {
			return nil, eris.Wrap(err, ErrFailedToCreateNewManager.Error())
		}
	} else {
		lm.currentBlock = storage.NewBlockID(lm.logFileName, logSize-1)
		err = lm.fm.Read(lm.currentBlock, lm.logPage)
		if err != nil {
			return nil, eris.Wrap(err, ErrFailedToCreateNewManager.Error())
		}
	}

	return lm, nil
}

// Flush сбрасывает журнал на диск
func (lm *Manager) Flush(lsn uint32, force bool) error {
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
	err := lm.Flush(0, true)
	if err != nil {
		return nil, err
	}

	it, err := NewIterator(lm.fm, lm.currentBlock)
	if err != nil {
		return nil, err
	}

	return it, nil
}

// Append добавляет в журнал новую запись
func (lm *Manager) Append(logRec []byte) (uint32, error) {
	lm.Lock()
	defer lm.Unlock()

	boundary := lm.logPage.GetUint32(blockStart)
	recsize := uint32(len(logRec))
	bytesNeeded := recsize + int32Size

	if int(boundary)-int(bytesNeeded) < int32Size {
		// Если данные не умещаются в блок, то:
		// — cбрасываем текущий блок на диск
		// — создаем новый блок
		err := lm.Flush(0, true)
		if err != nil {
			return 0, eris.Wrap(err, ErrFailedToAppendNewRecord.Error())
		}

		lm.currentBlock, err = lm.appendNewBlock()
		if err != nil {
			return 0, eris.Wrap(err, ErrFailedToAppendNewRecord.Error())
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
func (lm *Manager) appendNewBlock() (*storage.BlockID, error) {
	blk, err := lm.fm.Append(lm.logFileName)
	if err != nil {
		return nil, err
	}

	lm.logPage.SetUint32(blockStart, lm.fm.BlockSize())

	err = lm.fm.Write(blk, lm.logPage)
	if err != nil {
		return nil, err
	}

	return blk, nil
}
