package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/unhandled-exception/sophiadb/internal/pkg/test"
)

const testSuiteDir = "file_manager_tests"

type FileManagerTestSuite struct {
	suite.Suite
	suiteDir string
}

func TestFileManagerTestSuite(t *testing.T) {
	suite.Run(t, new(FileManagerTestSuite))
}

func (ts *FileManagerTestSuite) SuiteDir() string {
	return ts.suiteDir
}

func (ts *FileManagerTestSuite) SetupSuite() {
	ts.suiteDir = test.CreateSuiteTemporaryDir(ts, testSuiteDir)
}

func (ts *FileManagerTestSuite) TearDownSuite() {
	test.RemoveSuiteTemporaryDir(ts)
}

func (ts *FileManagerTestSuite) TestCreateFileManager() {
	path := filepath.Join(
		test.CreateTestTemporaryDir(ts),
		"data",
	)
	ts.Require().NoDirExists(path)
	fm, err := NewFileManager(path, 400)
	ts.DirExists(path)

	ts.Require().NoError(err)
	ts.Require().NotNil(fm)
	ts.Equal(uint32(400), fm.BlockSize())
	ts.Equal(path, fm.Path())
}

func (ts *FileManagerTestSuite) TestRemoveTemporaryFiles() {
	path := filepath.Join(test.CreateTestTemporaryDir(ts))

	// Создаем временные файлы в папке с тестом
	for i := 0; i < 5; i++ {
		test.CreateFile(ts, filepath.Join(path, fmt.Sprintf("%s_%d.dat", TempFilesPrefix, i)), []byte{})
	}

	for i := 0; i < 5; i++ {
		test.CreateFile(ts, filepath.Join(path, fmt.Sprintf("b_%d.dat", i)), []byte{})
	}

	dir, err := filepath.Glob(filepath.Join(path, fmt.Sprintf("%s_*.dat", TempFilesPrefix)))
	if err != nil {
		ts.FailNow(err.Error())
	}

	ts.Require().Len(dir, 5)

	fm, err := NewFileManager(path, 400)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	// Проверяем, что файлы удалили в конструкторе
	dir, err = filepath.Glob(filepath.Join(path, fmt.Sprintf("%s_*.dat", TempFilesPrefix)))
	if err != nil {
		ts.FailNow(err.Error())
	}

	ts.Require().Empty(dir)

	// Проверяем, что не удалили лишние файлы
	dir, err = filepath.Glob(filepath.Join(path, "b_*.dat"))
	if err != nil {
		ts.FailNow(err.Error())
	}

	ts.Require().Len(dir, 5)
}

func (ts *FileManagerTestSuite) TestCloseAllFiles() {
	path := filepath.Join(test.CreateTestTemporaryDir(ts))

	fm, err := NewFileManager(path, 400)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	// Создаем файлы с данными и записываем их в список файлов в менеджере
	for i := 0; i < 5; i++ {
		filePath := filepath.Join(path, fmt.Sprintf("%d.dat", i))
		test.CreateFile(ts, filePath, []byte{})

		f, err := os.Open(filePath)
		if err != nil {
			ts.FailNow("failed to open file \"%s\": %s", filePath, err.Error())
		}

		fm.openFiles[filepath.Base(filePath)] = f
	}
	ts.Require().Len(fm.openFiles, 5)

	err = fm.Close()
	ts.Require().NoError(err)
	ts.Require().Empty(fm.openFiles)

	// Создаем еще пару файлов и сразу закрываем их
	for i := 0; i < 2; i++ {
		filePath := filepath.Join(path, fmt.Sprintf("closed_%d.dat", i))
		test.CreateFile(ts, filePath, []byte{})

		f, err := os.Open(filePath)
		if err != nil {
			ts.FailNow("failed to open file \"%s\": %s", filePath, err.Error())
		}

		fm.openFiles[filepath.Base(filePath)] = f
		f.Close()
	}

	// Проверяем, что обрабатываем ошибки
	err = fm.Close()
	ts.Require().Error(err)
	ts.Require().ErrorIs(err, ErrFileManagerIO)
	ts.Require().Empty(fm.openFiles)
}

func (ts *FileManagerTestSuite) TestReadAndWriteBlocks() {
	path := filepath.Join(test.CreateTestTemporaryDir(ts))

	var blockSize uint32 = 100

	fm, err := NewFileManager(path, blockSize)
	ts.Require().NoError(err)

	ts.Require().NotNil(fm)

	defer fm.Close()

	// Создаем блоки
	blocks := make([]*BlockID, 10)

	for i := 0; i < len(blocks); i++ {
		filenum := i % 2

		blocks[i], err = fm.Append(fmt.Sprintf("b_%d.dat", filenum))
		if err != nil {
			ts.FailNow(err.Error())
		}
	}

	list, err := filepath.Glob(filepath.Join(path, "b_*.dat"))
	ts.Require().NoError(err)

	ts.Len(list, 2)

	// Создаем странички
	emptyPage := NewPage(blockSize)

	p1 := NewPage(blockSize)
	p1.SetString(0, "Первый блок")
	ts.Require().NotEqual(emptyPage.bb, p1.bb)

	p2 := NewPage(blockSize)
	p2.SetString(0, "Второй блок")
	ts.Require().NotEqual(emptyPage.bb, p2.bb)

	// Записываем странички в файлы
	ts.Require().NoError(fm.Write(blocks[0], p1))
	ts.Require().NoError(fm.Write(blocks[1], p1))
	ts.Require().NoError(fm.Write(blocks[2], p2))
	ts.Require().NoError(fm.Write(blocks[3], p2))

	// Страница-приёмник
	pd := NewPage(blockSize)

	// Читаем странички из файлов
	ts.Require().NoError(fm.Read(blocks[0], pd))
	ts.Equal(p1.Content(), pd.Content())

	ts.Require().NoError(fm.Read(blocks[2], pd))
	ts.Equal(p2.Content(), pd.Content())

	ts.Require().NoError(fm.Read(blocks[1], pd))
	ts.Equal(p1.Content(), pd.Content())

	ts.Require().NoError(fm.Read(blocks[3], pd))
	ts.Equal(p2.Content(), pd.Content())

	// Проверяем содержимое файла
	fc, err := ioutil.ReadFile(filepath.Join(path, blocks[0].Filename()))
	ts.Require().NoError(err)
	ts.Require().Len(fc, int(5*blockSize))
	ts.Equal(p1.bb, fc[0:blockSize])
	ts.Equal(p2.bb, fc[blockSize:2*blockSize])
	ts.Equal(emptyPage.bb, fc[2*blockSize:3*blockSize])
	ts.Equal(emptyPage.bb, fc[3*blockSize:4*blockSize])
	ts.Equal(emptyPage.bb, fc[4*blockSize:5*blockSize])
}
