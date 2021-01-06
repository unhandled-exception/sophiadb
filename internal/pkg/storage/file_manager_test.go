package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/rotisserie/eris"
	"github.com/stretchr/testify/suite"
)

type FileManagerTestSuite struct {
	suite.Suite
	suiteDir string
}

func TestFileManagerTestSuite(t *testing.T) {
	suite.Run(t, new(FileManagerTestSuite))
}

func (ts *FileManagerTestSuite) SetupSuite() {
	var err error
	ts.suiteDir, err = ioutil.TempDir("", "file_manager_tests_")
	if err != nil {
		ts.FailNow("dont create temporary folder: %s", err.Error())
	}
	ts.T().Logf("create suite temporary directory: %s", ts.suiteDir)
}

func (ts *FileManagerTestSuite) TearDownSuite() {
	err := os.RemoveAll(ts.suiteDir)
	if err != nil {
		ts.FailNow("dont remove temporary folder: %s", err.Error())
	}
	ts.T().Logf("remove suite temporary directory: %s", ts.suiteDir)
}

func (ts *FileManagerTestSuite) createTestTemporaryDir(testName string) string {
	path, err := ioutil.TempDir(ts.suiteDir, testName)
	if err != nil {
		ts.FailNowf("dont create test temporary dir: %s", err.Error())
	}
	ts.T().Logf("create test temporary directory: %s", path)
	return path
}

func (ts *FileManagerTestSuite) createFile(name string, content []byte) {
	file, err := os.Create(name)
	if err != nil {
		ts.FailNow("failed to create file \"%s\": %s", name, err)
	}
	defer file.Close()

	_, err = file.Write(content)
	if err != nil {
		ts.FailNow("failed to write content to file \"%s\": %s", name, err)
	}
}

func (ts *FileManagerTestSuite) TestCreateFileManager() {
	path := filepath.Join(
		ts.createTestTemporaryDir("TestCreateFileManager_"),
		"data",
	)
	ts.Require().NoDirExists(path)
	fm, err := NewFileManager(path, 400)
	ts.DirExists(path)

	ts.Require().NoError(err)
	ts.Require().NotNil(fm)
	ts.Equal(400, fm.BlockSize())
	ts.Equal(path, fm.Path())
}

func (ts *FileManagerTestSuite) TestRemoveTemporaryFiles() {
	path := filepath.Join(ts.createTestTemporaryDir("TestRemoveTemporaryFiles_"))

	// Создаем временные файлы в папке с тестом
	for i := 0; i < 5; i++ {
		ts.createFile(filepath.Join(path, fmt.Sprintf("%s_%d.dat", tempFilesPrefix, i)), []byte{})
	}
	for i := 0; i < 5; i++ {
		ts.createFile(filepath.Join(path, fmt.Sprintf("b_%d.dat", i)), []byte{})
	}
	dir, err := filepath.Glob(filepath.Join(path, fmt.Sprintf("%s_*.dat", tempFilesPrefix)))
	if err != nil {
		ts.FailNow(err.Error())
	}
	ts.Require().Len(dir, 5)

	fm, err := NewFileManager(path, 400)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	// Проверяем, что файлы удалили в конструкторе
	dir, err = filepath.Glob(filepath.Join(path, fmt.Sprintf("%s_*.dat", tempFilesPrefix)))
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
	path := filepath.Join(ts.createTestTemporaryDir("TestCloseAllFiles_"))

	fm, err := NewFileManager(path, 400)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	// Создаем файлы с данными и записываем их в список файлов в менеджере
	for i := 0; i < 5; i++ {
		filePath := filepath.Join(path, fmt.Sprintf("%d.dat", i))
		ts.createFile(filePath, []byte{})
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
		ts.createFile(filePath, []byte{})
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
	ts.Require().EqualError(eris.Cause(os.ErrClosed), os.ErrClosed.Error())
	ts.Require().Empty(fm.openFiles)
}

func (ts *FileManagerTestSuite) TestReadAndWriteBlocks() {
	path := filepath.Join(ts.createTestTemporaryDir("TestReadAndWriteBlocks_"))
	blockSize := 100

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
			ts.FailNow(eris.ToString(err, true))
		}
	}

	list, _ := filepath.Glob(filepath.Join(path, "b_*.dat"))
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
	ts.Require().Len(fc, 5*blockSize)
	ts.Equal(p1.bb, fc[0:blockSize])
	ts.Equal(p2.bb, fc[blockSize:2*blockSize])
	ts.Equal(emptyPage.bb, fc[2*blockSize:3*blockSize])
	ts.Equal(emptyPage.bb, fc[3*blockSize:4*blockSize])
	ts.Equal(emptyPage.bb, fc[4*blockSize:5*blockSize])
}
