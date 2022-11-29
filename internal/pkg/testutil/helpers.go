package testutil

import (
	"os"
	"strings"
	"testing"
)

type testSuite interface {
	T() *testing.T
	FailNow(failureMessage string, msgAndArgs ...any) bool
	SuiteDir() string
}

func CreateSuiteTemporaryDir(ts testSuite, name string) string {
	suiteDir, err := os.MkdirTemp("", name+"_")
	if err != nil {
		ts.FailNow("dont create temporary folder: %s", err.Error())
	}

	ts.T().Logf("create suite temporary directory: %s", suiteDir)

	return suiteDir
}

func RemoveSuiteTemporaryDir(ts testSuite) {
	err := os.RemoveAll(ts.SuiteDir())
	if err != nil {
		ts.FailNow("dont remove temporary folder: %s", err.Error())
	}

	ts.T().Logf("remove suite temporary directory: %s", ts.SuiteDir())
}

func CreateTestTemporaryDir(ts testSuite) string {
	path, err := os.MkdirTemp(ts.SuiteDir(), strings.ReplaceAll(ts.T().Name(), "/", "_")+"_")
	if err != nil {
		ts.FailNow("dont create test temporary dir: %s", err.Error())
	}

	ts.T().Logf("create test temporary directory: %s", path)

	return path
}

func CreateFile(ts testSuite, name string, content []byte) {
	file, err := os.Create(name)
	if err != nil {
		ts.FailNow("failed to create file \"%s\": %s", name, err)
	}

	defer file.Close()

	_, err = file.Write(content)
	if err != nil {
		ts.FailNow("failed to write content to file \"%s\": %s", name, err)
	}

	ts.T().Logf("create file: %s, length: %d", name, len(content))
}

func GetFileSize(ts testSuite, name string) int64 {
	stat, err := os.Stat(name)
	if err != nil {
		ts.FailNow(err.Error())
	}

	return stat.Size()
}
