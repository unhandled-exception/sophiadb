package storage

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
	"github.com/unhandled-exception/sophiadb/internal/pkg/utils"
)

const (
	// TempFilesPrefix — префикс для временных файлов
	TempFilesPrefix = "temp_"

	defaultFilePermissions = 0o700
	syncedFilePermissions  = 0o755
)

type OpenFilesMap map[string]*os.File

// Manager управляет чтением и записью блоков на диске
type Manager struct {
	mu sync.Mutex

	IsNew bool

	path      string
	blockSize uint32
	openFiles OpenFilesMap
}

// NewFileManager создает новый объект FileManager
func NewFileManager(path string, blockSize uint32) (*Manager, error) {
	fm := &Manager{
		path:      path,
		blockSize: blockSize,
		openFiles: make(OpenFilesMap),
		IsNew:     true,
	}

	if lstat, err := os.Lstat(path); err == nil && lstat.IsDir() {
		fm.IsNew = false
	}

	if err := os.MkdirAll(path, defaultFilePermissions); err != nil {
		return nil, errors.WithMessagef(ErrFileManagerIO, "cannot create data dir \"%s\": %v", path, err)
	}

	if err := fm.cleanTemporaryFiles(); err != nil {
		return nil, err
	}

	return fm, nil
}

func (fm *Manager) OpenFiles() OpenFilesMap {
	return fm.openFiles
}

// cleanTemporaryFiles
func (fm *Manager) cleanTemporaryFiles() error {
	return filepath.Walk(
		fm.path,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && strings.HasPrefix(info.Name(), TempFilesPrefix) {
				err := os.Remove(path)

				return err
			}

			return nil
		},
	)
}

// BlockSize возвращает размер блока
func (fm *Manager) BlockSize() uint32 {
	return fm.blockSize
}

// Path возвращает путь к папке с данными
func (fm *Manager) Path() string {
	return fm.path
}

// Close pаскрывает файлы открытые менеджером
func (fm *Manager) Close() error {
	errs := make([]error, 0, len(fm.openFiles))

	var err error
	for k, v := range fm.openFiles {
		err = v.Close()
		if err != nil {
			errs = append(errs, err)
		}

		delete(fm.openFiles, k)
	}

	if len(errs) > 0 {
		return errors.WithMessagef(ErrFileManagerIO, "errors on close files: %s", utils.JoinErrors(errs, ", "))
	}

	return nil
}

// Read читает блок из файла в страницу page
func (fm *Manager) Read(block types.Block, page *types.Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	file, err := fm.getFile(block.Filename)
	if err != nil {
		return err
	}

	_, err = file.Seek(int64(block.Number)*int64(fm.blockSize), io.SeekStart)
	if err != nil {
		return errors.WithMessage(ErrFileManagerIO, err.Error())
	}

	_, err = file.Read(page.Content())
	if err != nil {
		return errors.WithMessage(ErrFileManagerIO, err.Error())
	}

	return nil
}

// Write записывает блок в файл из страницы page
func (fm *Manager) Write(block types.Block, page *types.Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	file, err := fm.getFile(block.Filename)
	if err != nil {
		return err
	}

	_, err = file.Seek(int64(block.Number)*int64(fm.blockSize), io.SeekStart)
	if err != nil {
		return errors.WithMessage(ErrFileManagerIO, err.Error())
	}

	_, err = file.Write(page.Content())
	if err != nil {
		return errors.WithMessage(ErrFileManagerIO, err.Error())
	}

	return nil
}

// Append добавляет новый блок в файл
func (fm *Manager) Append(filename string) (types.Block, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	block := types.Block{}

	blkNum, err := fm.Length(filename)
	if err != nil {
		return block, err
	}

	block.Filename = filename
	block.Number = blkNum

	blockData := make([]byte, fm.blockSize)

	file, err := fm.getFile(block.Filename)
	if err != nil {
		return block, err
	}

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return block, errors.WithMessage(ErrFileManagerIO, err.Error())
	}

	if _, err := file.Write(blockData); err != nil {
		return block, errors.WithMessage(ErrFileManagerIO, err.Error())
	}

	return block, nil
}

// Length возвращает размер файла в блоках
func (fm *Manager) Length(filename string) (types.BlockID, error) {
	file, err := fm.getFile(filename)
	if err != nil {
		return 0, err
	}

	stat, err := file.Stat()
	if err != nil {
		return 0, errors.WithMessage(ErrFileManagerIO, err.Error())
	}

	return types.BlockID(int32(stat.Size() / int64(fm.blockSize))), nil
}

// getFile возвращает файл из списка открытых или
func (fm *Manager) getFile(filename string) (*os.File, error) {
	file, ok := fm.openFiles[filename]
	if !ok {
		var err error
		// Создаем файл без локов. Локи нужно делать в вызывающих методах
		file, err = os.OpenFile(
			filepath.Join(fm.path, filename),
			os.O_CREATE|os.O_RDWR|os.O_SYNC, // Открываем файл в режим O_SYNC, чтобы выполнялся автоматический флаш данных при чтении и записи
			syncedFilePermissions,
		)
		if err != nil {
			return nil, errors.WithMessage(ErrFileManagerIO, err.Error())
		}

		fm.openFiles[filename] = file
	}

	return file, nil
}
