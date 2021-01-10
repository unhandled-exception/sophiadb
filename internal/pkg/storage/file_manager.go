package storage

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rotisserie/eris"
)

const (
	// TempFilesPrefix — префикс для временных файлов
	TempFilesPrefix = "temp"

	defaultFilePermissions = 0600
)

// ErrFileManagerIO вызываем при ошибках ввода вывода
var ErrFileManagerIO error = eris.New("file manager io error")

type openFilesMap map[string]*os.File

// FileManager управляет чтением и записью блоков на диске
type FileManager struct {
	sync.Mutex

	path      string
	blockSize uint32
	openFiles openFilesMap
}

// NewFileManager создает новый объект FileManager
func NewFileManager(path string, blockSize uint32) (*FileManager, error) {
	var err error
	fm := &FileManager{
		path:      path,
		blockSize: blockSize,
		openFiles: make(openFilesMap),
	}

	err = os.MkdirAll(path, defaultFilePermissions)
	if err != nil {
		return nil, eris.Wrapf(err, "file manager: cannot create data dir \"%s\"", path)
	}

	err = fm.cleanTemporaryFiles()
	if err != nil {
		return nil, err
	}

	return fm, nil
}

// cleanTemporaryFiles
func (fm *FileManager) cleanTemporaryFiles() error {
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
func (fm *FileManager) BlockSize() uint32 {
	return fm.blockSize
}

// Path возвращает путь к папке с данными
func (fm *FileManager) Path() string {
	return fm.path
}

// Close pаскрывает файлы открытые менеджером
func (fm *FileManager) Close() error {
	errors := make([]error, 0, len(fm.openFiles))

	var err error
	for k, v := range fm.openFiles {
		err = v.Close()
		if err != nil {
			errors = append(errors, err)
		}
		delete(fm.openFiles, k)
	}

	if len(errors) > 0 {
		return eris.Errorf("file manager: errors on close files: %s", joinErrors(errors, ", "))
	}
	return nil
}

// Read читает блок из файла в страницу page
func (fm *FileManager) Read(block *BlockID, page *Page) error {
	fm.Lock()
	defer fm.Unlock()

	file, err := fm.getFile(block.Filename())
	if err != nil {
		return err
	}
	_, err = file.Seek(int64(block.number)*int64(fm.blockSize), io.SeekStart)
	if err != nil {
		return eris.Wrap(err, ErrFileManagerIO.Error())
	}
	_, err = file.Read(page.Content())
	if err != nil {
		return eris.Wrap(err, ErrFileManagerIO.Error())
	}
	return nil
}

// Write записывает блок в файл из страницы page
func (fm *FileManager) Write(block *BlockID, page *Page) error {
	fm.Lock()
	defer fm.Unlock()
	file, err := fm.getFile(block.Filename())
	if err != nil {
		return err
	}
	_, err = file.Seek(int64(block.number)*int64(fm.blockSize), io.SeekStart)
	if err != nil {
		return eris.Wrap(err, ErrFileManagerIO.Error())
	}
	_, err = file.Write(page.Content())
	if err != nil {
		return eris.Wrap(err, ErrFileManagerIO.Error())
	}
	return nil
}

// Append добавляет новый блок в файл
func (fm *FileManager) Append(filename string) (*BlockID, error) {
	fm.Lock()
	defer fm.Unlock()

	blkNum, err := fm.Length(filename)
	if err != nil {
		return nil, err
	}

	blockID := NewBlockID(filename, blkNum)
	blockData := make([]byte, fm.blockSize)

	file, err := fm.getFile(blockID.Filename())
	if err != nil {
		return nil, err
	}

	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, eris.Wrap(err, ErrFileManagerIO.Error())
	}
	_, err = file.Write(blockData)
	if err != nil {
		return nil, eris.Wrap(err, ErrFileManagerIO.Error())
	}

	return blockID, nil
}

// Length возвращает размер файла в блоках
func (fm *FileManager) Length(filename string) (uint32, error) {
	file, err := fm.getFile(filename)
	if err != nil {
		return 0, err
	}
	stat, err := file.Stat()
	if err != nil {
		return 0, eris.Wrap(err, ErrFileManagerIO.Error())
	}
	return uint32(stat.Size() / int64(fm.blockSize)), nil
}

// getFile возвращает файл из списка открытых или
func (fm *FileManager) getFile(filename string) (*os.File, error) {
	file, ok := fm.openFiles[filename]
	if !ok {
		var err error
		// Создаем файл без локов. Локи нужно делать в вызывающих методах
		file, err = os.OpenFile(
			filepath.Join(fm.path, filename),
			os.O_CREATE|os.O_RDWR|os.O_SYNC, // Открываем файл в режим O_SYNC, чтобы выполнялся автоматический флаш данных при чтении и записи
			0755,
		)
		if err != nil {
			return nil, eris.Wrap(err, "file manager create new file errror")
		}
		fm.openFiles[filename] = file
	}
	return file, nil
}
