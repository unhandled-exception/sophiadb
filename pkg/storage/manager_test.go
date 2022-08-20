package storage_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/unhandled-exception/sophiadb/pkg/storage"
	"github.com/unhandled-exception/sophiadb/pkg/testutil"
	"github.com/unhandled-exception/sophiadb/pkg/types"
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
	ts.suiteDir = testutil.CreateSuiteTemporaryDir(ts, testSuiteDir)
}

func (ts *FileManagerTestSuite) TearDownSuite() {
	testutil.RemoveSuiteTemporaryDir(ts)
}

func (ts *FileManagerTestSuite) TestCreateFileManager() {
	path := filepath.Join(
		testutil.CreateTestTemporaryDir(ts),
		"data",
	)
	ts.Require().NoDirExists(path)
	fm, err := storage.NewFileManager(path, 400)
	ts.DirExists(path)

	ts.Require().NoError(err)
	ts.Require().NotNil(fm)
	ts.Equal(uint32(400), fm.BlockSize())
	ts.Equal(path, fm.Path())
	ts.True(fm.IsNew)
}

func (ts *FileManagerTestSuite) TestCreateFileManagerOnExistsFolder() {
	path := filepath.Join(
		testutil.CreateTestTemporaryDir(ts),
		"data",
	)
	_, err := storage.NewFileManager(path, 400)
	ts.Require().NoError(err)

	ts.Require().DirExists(path)
	sut, err := storage.NewFileManager(path, 400)
	ts.Require().NoError(err)

	ts.False(sut.IsNew)
}

func (ts *FileManagerTestSuite) TestRemoveTemporaryFiles() {
	path := filepath.Join(testutil.CreateTestTemporaryDir(ts))

	// Создаем временные файлы в папке с тестом
	for i := 0; i < 5; i++ {
		testutil.CreateFile(ts, filepath.Join(path, fmt.Sprintf("%s_%d.dat", storage.TempFilesPrefix, i)), []byte{})
	}

	for i := 0; i < 5; i++ {
		testutil.CreateFile(ts, filepath.Join(path, fmt.Sprintf("b_%d.dat", i)), []byte{})
	}

	dir, err := filepath.Glob(filepath.Join(path, fmt.Sprintf("%s_*.dat", storage.TempFilesPrefix)))
	if err != nil {
		ts.FailNow(err.Error())
	}

	ts.Require().Len(dir, 5)

	fm, err := storage.NewFileManager(path, 400)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	// Проверяем, что файлы удалили в конструкторе
	dir, err = filepath.Glob(filepath.Join(path, fmt.Sprintf("%s_*.dat", storage.TempFilesPrefix)))
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
	path := filepath.Join(testutil.CreateTestTemporaryDir(ts))

	fm, err := storage.NewFileManager(path, 400)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	// Создаем файлы с данными и записываем их в список файлов в менеджере
	for i := 0; i < 5; i++ {
		filePath := filepath.Join(path, fmt.Sprintf("%d.dat", i))
		testutil.CreateFile(ts, filePath, []byte{})

		f, nerr := os.Open(filePath)
		if nerr != nil {
			ts.FailNow("failed to open file \"%s\": %s", filePath, nerr.Error())
		}

		fm.OpenFiles()[filepath.Base(filePath)] = f
	}
	ts.Require().Len(fm.OpenFiles(), 5)

	err = fm.Close()
	ts.Require().NoError(err)
	ts.Require().Empty(fm.OpenFiles())

	// Создаем еще пару файлов и сразу закрываем их
	for i := 0; i < 2; i++ {
		filePath := filepath.Join(path, fmt.Sprintf("closed_%d.dat", i))
		testutil.CreateFile(ts, filePath, []byte{})

		f, nerr := os.Open(filePath)
		if nerr != nil {
			ts.FailNow("failed to open file \"%s\": %s", filePath, nerr.Error())
		}

		fm.OpenFiles()[filepath.Base(filePath)] = f
		f.Close()
	}

	// Проверяем, что обрабатываем ошибки
	err = fm.Close()
	ts.Require().Error(err)
	ts.Require().ErrorIs(err, storage.ErrFileManagerIO)
	ts.Require().Empty(fm.OpenFiles())
}

func (ts *FileManagerTestSuite) TestReadAndWriteBlocks() {
	path := filepath.Join(testutil.CreateTestTemporaryDir(ts))

	var blockSize uint32 = 100

	fm, err := storage.NewFileManager(path, blockSize)
	ts.Require().NoError(err)

	ts.Require().NotNil(fm)

	defer fm.Close()

	// Создаем блоки
	blocks := make([]types.Block, 10)

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
	emptyPage := types.NewPage(blockSize)

	p1 := types.NewPage(blockSize)
	p1.SetString(0, "Первый блок")
	ts.Require().NotEqual(emptyPage.Content(), p1.Content())

	p2 := types.NewPage(blockSize)
	p2.SetString(0, "Второй блок")
	ts.Require().NotEqual(emptyPage.Content(), p2.Content())

	// Записываем странички в файлы
	ts.Require().NoError(fm.Write(blocks[0], p1))
	ts.Require().NoError(fm.Write(blocks[1], p1))
	ts.Require().NoError(fm.Write(blocks[2], p2))
	ts.Require().NoError(fm.Write(blocks[3], p2))

	// Страница-приёмник
	pd := types.NewPage(blockSize)

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
	fc, err := os.ReadFile(filepath.Join(path, blocks[0].Filename))
	ts.Require().NoError(err)
	ts.Require().Len(fc, int(5*blockSize))
	ts.Equal(p1.Content(), fc[0:blockSize])
	ts.Equal(p2.Content(), fc[blockSize:2*blockSize])
	ts.Equal(emptyPage.Content(), fc[2*blockSize:3*blockSize])
	ts.Equal(emptyPage.Content(), fc[3*blockSize:4*blockSize])
	ts.Equal(emptyPage.Content(), fc[4*blockSize:5*blockSize])
}
