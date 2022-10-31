package testutil

import "github.com/stretchr/testify/suite"

type Suite struct {
	suite.Suite

	suiteDir       string
	suiteDirPrefix string
}

func (ts *Suite) SuiteDir() string {
	return ts.suiteDir
}

func (ts *Suite) SetupSuite() {
	ts.suiteDir = CreateSuiteTemporaryDir(ts, ts.suiteDirPrefix)
}

func (ts *Suite) TearDownSuite() {
	RemoveSuiteTemporaryDir(ts)
}

func (ts *Suite) CreateTestTemporaryDir() string {
	return CreateTestTemporaryDir(ts)
}
