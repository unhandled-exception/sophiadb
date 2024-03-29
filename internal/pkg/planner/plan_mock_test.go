package planner

// Code generated by http://github.com/gojuno/minimock (dev). DO NOT EDIT.

//go:generate minimock -i github.com/unhandled-exception/sophiadb/internal/pkg/planner.Plan -o ./plan_mock_test.go -n PlanMock

import (
	"sync"
	mm_atomic "sync/atomic"
	mm_time "time"

	"github.com/gojuno/minimock/v3"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

// PlanMock implements Plan
type PlanMock struct {
	t minimock.Tester

	funcBlocksAccessed          func() (i1 int64)
	inspectFuncBlocksAccessed   func()
	afterBlocksAccessedCounter  uint64
	beforeBlocksAccessedCounter uint64
	BlocksAccessedMock          mPlanMockBlocksAccessed

	funcDistinctValues          func(s1 string) (i1 int64, b1 bool)
	inspectFuncDistinctValues   func(s1 string)
	afterDistinctValuesCounter  uint64
	beforeDistinctValuesCounter uint64
	DistinctValuesMock          mPlanMockDistinctValues

	funcOpen          func() (s1 scan.Scan, err error)
	inspectFuncOpen   func()
	afterOpenCounter  uint64
	beforeOpenCounter uint64
	OpenMock          mPlanMockOpen

	funcRecords          func() (i1 int64)
	inspectFuncRecords   func()
	afterRecordsCounter  uint64
	beforeRecordsCounter uint64
	RecordsMock          mPlanMockRecords

	funcSchema          func() (s1 records.Schema)
	inspectFuncSchema   func()
	afterSchemaCounter  uint64
	beforeSchemaCounter uint64
	SchemaMock          mPlanMockSchema

	funcString          func() (s1 string)
	inspectFuncString   func()
	afterStringCounter  uint64
	beforeStringCounter uint64
	StringMock          mPlanMockString
}

// NewPlanMock returns a mock for Plan
func NewPlanMock(t minimock.Tester) *PlanMock {
	m := &PlanMock{t: t}
	if controller, ok := t.(minimock.MockController); ok {
		controller.RegisterMocker(m)
	}

	m.BlocksAccessedMock = mPlanMockBlocksAccessed{mock: m}

	m.DistinctValuesMock = mPlanMockDistinctValues{mock: m}
	m.DistinctValuesMock.callArgs = []*PlanMockDistinctValuesParams{}

	m.OpenMock = mPlanMockOpen{mock: m}

	m.RecordsMock = mPlanMockRecords{mock: m}

	m.SchemaMock = mPlanMockSchema{mock: m}

	m.StringMock = mPlanMockString{mock: m}

	return m
}

type mPlanMockBlocksAccessed struct {
	mock               *PlanMock
	defaultExpectation *PlanMockBlocksAccessedExpectation
	expectations       []*PlanMockBlocksAccessedExpectation
}

// PlanMockBlocksAccessedExpectation specifies expectation struct of the Plan.BlocksAccessed
type PlanMockBlocksAccessedExpectation struct {
	mock *PlanMock

	results *PlanMockBlocksAccessedResults
	Counter uint64
}

// PlanMockBlocksAccessedResults contains results of the Plan.BlocksAccessed
type PlanMockBlocksAccessedResults struct {
	i1 int64
}

// Expect sets up expected params for Plan.BlocksAccessed
func (mmBlocksAccessed *mPlanMockBlocksAccessed) Expect() *mPlanMockBlocksAccessed {
	if mmBlocksAccessed.mock.funcBlocksAccessed != nil {
		mmBlocksAccessed.mock.t.Fatalf("PlanMock.BlocksAccessed mock is already set by Set")
	}

	if mmBlocksAccessed.defaultExpectation == nil {
		mmBlocksAccessed.defaultExpectation = &PlanMockBlocksAccessedExpectation{}
	}

	return mmBlocksAccessed
}

// Inspect accepts an inspector function that has same arguments as the Plan.BlocksAccessed
func (mmBlocksAccessed *mPlanMockBlocksAccessed) Inspect(f func()) *mPlanMockBlocksAccessed {
	if mmBlocksAccessed.mock.inspectFuncBlocksAccessed != nil {
		mmBlocksAccessed.mock.t.Fatalf("Inspect function is already set for PlanMock.BlocksAccessed")
	}

	mmBlocksAccessed.mock.inspectFuncBlocksAccessed = f

	return mmBlocksAccessed
}

// Return sets up results that will be returned by Plan.BlocksAccessed
func (mmBlocksAccessed *mPlanMockBlocksAccessed) Return(i1 int64) *PlanMock {
	if mmBlocksAccessed.mock.funcBlocksAccessed != nil {
		mmBlocksAccessed.mock.t.Fatalf("PlanMock.BlocksAccessed mock is already set by Set")
	}

	if mmBlocksAccessed.defaultExpectation == nil {
		mmBlocksAccessed.defaultExpectation = &PlanMockBlocksAccessedExpectation{mock: mmBlocksAccessed.mock}
	}
	mmBlocksAccessed.defaultExpectation.results = &PlanMockBlocksAccessedResults{i1}
	return mmBlocksAccessed.mock
}

//Set uses given function f to mock the Plan.BlocksAccessed method
func (mmBlocksAccessed *mPlanMockBlocksAccessed) Set(f func() (i1 int64)) *PlanMock {
	if mmBlocksAccessed.defaultExpectation != nil {
		mmBlocksAccessed.mock.t.Fatalf("Default expectation is already set for the Plan.BlocksAccessed method")
	}

	if len(mmBlocksAccessed.expectations) > 0 {
		mmBlocksAccessed.mock.t.Fatalf("Some expectations are already set for the Plan.BlocksAccessed method")
	}

	mmBlocksAccessed.mock.funcBlocksAccessed = f
	return mmBlocksAccessed.mock
}

// BlocksAccessed implements Plan
func (mmBlocksAccessed *PlanMock) BlocksAccessed() (i1 int64) {
	mm_atomic.AddUint64(&mmBlocksAccessed.beforeBlocksAccessedCounter, 1)
	defer mm_atomic.AddUint64(&mmBlocksAccessed.afterBlocksAccessedCounter, 1)

	if mmBlocksAccessed.inspectFuncBlocksAccessed != nil {
		mmBlocksAccessed.inspectFuncBlocksAccessed()
	}

	if mmBlocksAccessed.BlocksAccessedMock.defaultExpectation != nil {
		mm_atomic.AddUint64(&mmBlocksAccessed.BlocksAccessedMock.defaultExpectation.Counter, 1)

		mm_results := mmBlocksAccessed.BlocksAccessedMock.defaultExpectation.results
		if mm_results == nil {
			mmBlocksAccessed.t.Fatal("No results are set for the PlanMock.BlocksAccessed")
		}
		return (*mm_results).i1
	}
	if mmBlocksAccessed.funcBlocksAccessed != nil {
		return mmBlocksAccessed.funcBlocksAccessed()
	}
	mmBlocksAccessed.t.Fatalf("Unexpected call to PlanMock.BlocksAccessed.")
	return
}

// BlocksAccessedAfterCounter returns a count of finished PlanMock.BlocksAccessed invocations
func (mmBlocksAccessed *PlanMock) BlocksAccessedAfterCounter() uint64 {
	return mm_atomic.LoadUint64(&mmBlocksAccessed.afterBlocksAccessedCounter)
}

// BlocksAccessedBeforeCounter returns a count of PlanMock.BlocksAccessed invocations
func (mmBlocksAccessed *PlanMock) BlocksAccessedBeforeCounter() uint64 {
	return mm_atomic.LoadUint64(&mmBlocksAccessed.beforeBlocksAccessedCounter)
}

// MinimockBlocksAccessedDone returns true if the count of the BlocksAccessed invocations corresponds
// the number of defined expectations
func (m *PlanMock) MinimockBlocksAccessedDone() bool {
	for _, e := range m.BlocksAccessedMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			return false
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.BlocksAccessedMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterBlocksAccessedCounter) < 1 {
		return false
	}
	// if func was set then invocations count should be greater than zero
	if m.funcBlocksAccessed != nil && mm_atomic.LoadUint64(&m.afterBlocksAccessedCounter) < 1 {
		return false
	}
	return true
}

// MinimockBlocksAccessedInspect logs each unmet expectation
func (m *PlanMock) MinimockBlocksAccessedInspect() {
	for _, e := range m.BlocksAccessedMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			m.t.Error("Expected call to PlanMock.BlocksAccessed")
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.BlocksAccessedMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterBlocksAccessedCounter) < 1 {
		m.t.Error("Expected call to PlanMock.BlocksAccessed")
	}
	// if func was set then invocations count should be greater than zero
	if m.funcBlocksAccessed != nil && mm_atomic.LoadUint64(&m.afterBlocksAccessedCounter) < 1 {
		m.t.Error("Expected call to PlanMock.BlocksAccessed")
	}
}

type mPlanMockDistinctValues struct {
	mock               *PlanMock
	defaultExpectation *PlanMockDistinctValuesExpectation
	expectations       []*PlanMockDistinctValuesExpectation

	callArgs []*PlanMockDistinctValuesParams
	mutex    sync.RWMutex
}

// PlanMockDistinctValuesExpectation specifies expectation struct of the Plan.DistinctValues
type PlanMockDistinctValuesExpectation struct {
	mock    *PlanMock
	params  *PlanMockDistinctValuesParams
	results *PlanMockDistinctValuesResults
	Counter uint64
}

// PlanMockDistinctValuesParams contains parameters of the Plan.DistinctValues
type PlanMockDistinctValuesParams struct {
	s1 string
}

// PlanMockDistinctValuesResults contains results of the Plan.DistinctValues
type PlanMockDistinctValuesResults struct {
	i1 int64
	b1 bool
}

// Expect sets up expected params for Plan.DistinctValues
func (mmDistinctValues *mPlanMockDistinctValues) Expect(s1 string) *mPlanMockDistinctValues {
	if mmDistinctValues.mock.funcDistinctValues != nil {
		mmDistinctValues.mock.t.Fatalf("PlanMock.DistinctValues mock is already set by Set")
	}

	if mmDistinctValues.defaultExpectation == nil {
		mmDistinctValues.defaultExpectation = &PlanMockDistinctValuesExpectation{}
	}

	mmDistinctValues.defaultExpectation.params = &PlanMockDistinctValuesParams{s1}
	for _, e := range mmDistinctValues.expectations {
		if minimock.Equal(e.params, mmDistinctValues.defaultExpectation.params) {
			mmDistinctValues.mock.t.Fatalf("Expectation set by When has same params: %#v", *mmDistinctValues.defaultExpectation.params)
		}
	}

	return mmDistinctValues
}

// Inspect accepts an inspector function that has same arguments as the Plan.DistinctValues
func (mmDistinctValues *mPlanMockDistinctValues) Inspect(f func(s1 string)) *mPlanMockDistinctValues {
	if mmDistinctValues.mock.inspectFuncDistinctValues != nil {
		mmDistinctValues.mock.t.Fatalf("Inspect function is already set for PlanMock.DistinctValues")
	}

	mmDistinctValues.mock.inspectFuncDistinctValues = f

	return mmDistinctValues
}

// Return sets up results that will be returned by Plan.DistinctValues
func (mmDistinctValues *mPlanMockDistinctValues) Return(i1 int64, b1 bool) *PlanMock {
	if mmDistinctValues.mock.funcDistinctValues != nil {
		mmDistinctValues.mock.t.Fatalf("PlanMock.DistinctValues mock is already set by Set")
	}

	if mmDistinctValues.defaultExpectation == nil {
		mmDistinctValues.defaultExpectation = &PlanMockDistinctValuesExpectation{mock: mmDistinctValues.mock}
	}
	mmDistinctValues.defaultExpectation.results = &PlanMockDistinctValuesResults{i1, b1}
	return mmDistinctValues.mock
}

//Set uses given function f to mock the Plan.DistinctValues method
func (mmDistinctValues *mPlanMockDistinctValues) Set(f func(s1 string) (i1 int64, b1 bool)) *PlanMock {
	if mmDistinctValues.defaultExpectation != nil {
		mmDistinctValues.mock.t.Fatalf("Default expectation is already set for the Plan.DistinctValues method")
	}

	if len(mmDistinctValues.expectations) > 0 {
		mmDistinctValues.mock.t.Fatalf("Some expectations are already set for the Plan.DistinctValues method")
	}

	mmDistinctValues.mock.funcDistinctValues = f
	return mmDistinctValues.mock
}

// When sets expectation for the Plan.DistinctValues which will trigger the result defined by the following
// Then helper
func (mmDistinctValues *mPlanMockDistinctValues) When(s1 string) *PlanMockDistinctValuesExpectation {
	if mmDistinctValues.mock.funcDistinctValues != nil {
		mmDistinctValues.mock.t.Fatalf("PlanMock.DistinctValues mock is already set by Set")
	}

	expectation := &PlanMockDistinctValuesExpectation{
		mock:   mmDistinctValues.mock,
		params: &PlanMockDistinctValuesParams{s1},
	}
	mmDistinctValues.expectations = append(mmDistinctValues.expectations, expectation)
	return expectation
}

// Then sets up Plan.DistinctValues return parameters for the expectation previously defined by the When method
func (e *PlanMockDistinctValuesExpectation) Then(i1 int64, b1 bool) *PlanMock {
	e.results = &PlanMockDistinctValuesResults{i1, b1}
	return e.mock
}

// DistinctValues implements Plan
func (mmDistinctValues *PlanMock) DistinctValues(s1 string) (i1 int64, b1 bool) {
	mm_atomic.AddUint64(&mmDistinctValues.beforeDistinctValuesCounter, 1)
	defer mm_atomic.AddUint64(&mmDistinctValues.afterDistinctValuesCounter, 1)

	if mmDistinctValues.inspectFuncDistinctValues != nil {
		mmDistinctValues.inspectFuncDistinctValues(s1)
	}

	mm_params := &PlanMockDistinctValuesParams{s1}

	// Record call args
	mmDistinctValues.DistinctValuesMock.mutex.Lock()
	mmDistinctValues.DistinctValuesMock.callArgs = append(mmDistinctValues.DistinctValuesMock.callArgs, mm_params)
	mmDistinctValues.DistinctValuesMock.mutex.Unlock()

	for _, e := range mmDistinctValues.DistinctValuesMock.expectations {
		if minimock.Equal(e.params, mm_params) {
			mm_atomic.AddUint64(&e.Counter, 1)
			return e.results.i1, e.results.b1
		}
	}

	if mmDistinctValues.DistinctValuesMock.defaultExpectation != nil {
		mm_atomic.AddUint64(&mmDistinctValues.DistinctValuesMock.defaultExpectation.Counter, 1)
		mm_want := mmDistinctValues.DistinctValuesMock.defaultExpectation.params
		mm_got := PlanMockDistinctValuesParams{s1}
		if mm_want != nil && !minimock.Equal(*mm_want, mm_got) {
			mmDistinctValues.t.Errorf("PlanMock.DistinctValues got unexpected parameters, want: %#v, got: %#v%s\n", *mm_want, mm_got, minimock.Diff(*mm_want, mm_got))
		}

		mm_results := mmDistinctValues.DistinctValuesMock.defaultExpectation.results
		if mm_results == nil {
			mmDistinctValues.t.Fatal("No results are set for the PlanMock.DistinctValues")
		}
		return (*mm_results).i1, (*mm_results).b1
	}
	if mmDistinctValues.funcDistinctValues != nil {
		return mmDistinctValues.funcDistinctValues(s1)
	}
	mmDistinctValues.t.Fatalf("Unexpected call to PlanMock.DistinctValues. %v", s1)
	return
}

// DistinctValuesAfterCounter returns a count of finished PlanMock.DistinctValues invocations
func (mmDistinctValues *PlanMock) DistinctValuesAfterCounter() uint64 {
	return mm_atomic.LoadUint64(&mmDistinctValues.afterDistinctValuesCounter)
}

// DistinctValuesBeforeCounter returns a count of PlanMock.DistinctValues invocations
func (mmDistinctValues *PlanMock) DistinctValuesBeforeCounter() uint64 {
	return mm_atomic.LoadUint64(&mmDistinctValues.beforeDistinctValuesCounter)
}

// Calls returns a list of arguments used in each call to PlanMock.DistinctValues.
// The list is in the same order as the calls were made (i.e. recent calls have a higher index)
func (mmDistinctValues *mPlanMockDistinctValues) Calls() []*PlanMockDistinctValuesParams {
	mmDistinctValues.mutex.RLock()

	argCopy := make([]*PlanMockDistinctValuesParams, len(mmDistinctValues.callArgs))
	copy(argCopy, mmDistinctValues.callArgs)

	mmDistinctValues.mutex.RUnlock()

	return argCopy
}

// MinimockDistinctValuesDone returns true if the count of the DistinctValues invocations corresponds
// the number of defined expectations
func (m *PlanMock) MinimockDistinctValuesDone() bool {
	for _, e := range m.DistinctValuesMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			return false
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.DistinctValuesMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterDistinctValuesCounter) < 1 {
		return false
	}
	// if func was set then invocations count should be greater than zero
	if m.funcDistinctValues != nil && mm_atomic.LoadUint64(&m.afterDistinctValuesCounter) < 1 {
		return false
	}
	return true
}

// MinimockDistinctValuesInspect logs each unmet expectation
func (m *PlanMock) MinimockDistinctValuesInspect() {
	for _, e := range m.DistinctValuesMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			m.t.Errorf("Expected call to PlanMock.DistinctValues with params: %#v", *e.params)
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.DistinctValuesMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterDistinctValuesCounter) < 1 {
		if m.DistinctValuesMock.defaultExpectation.params == nil {
			m.t.Error("Expected call to PlanMock.DistinctValues")
		} else {
			m.t.Errorf("Expected call to PlanMock.DistinctValues with params: %#v", *m.DistinctValuesMock.defaultExpectation.params)
		}
	}
	// if func was set then invocations count should be greater than zero
	if m.funcDistinctValues != nil && mm_atomic.LoadUint64(&m.afterDistinctValuesCounter) < 1 {
		m.t.Error("Expected call to PlanMock.DistinctValues")
	}
}

type mPlanMockOpen struct {
	mock               *PlanMock
	defaultExpectation *PlanMockOpenExpectation
	expectations       []*PlanMockOpenExpectation
}

// PlanMockOpenExpectation specifies expectation struct of the Plan.Open
type PlanMockOpenExpectation struct {
	mock *PlanMock

	results *PlanMockOpenResults
	Counter uint64
}

// PlanMockOpenResults contains results of the Plan.Open
type PlanMockOpenResults struct {
	s1  scan.Scan
	err error
}

// Expect sets up expected params for Plan.Open
func (mmOpen *mPlanMockOpen) Expect() *mPlanMockOpen {
	if mmOpen.mock.funcOpen != nil {
		mmOpen.mock.t.Fatalf("PlanMock.Open mock is already set by Set")
	}

	if mmOpen.defaultExpectation == nil {
		mmOpen.defaultExpectation = &PlanMockOpenExpectation{}
	}

	return mmOpen
}

// Inspect accepts an inspector function that has same arguments as the Plan.Open
func (mmOpen *mPlanMockOpen) Inspect(f func()) *mPlanMockOpen {
	if mmOpen.mock.inspectFuncOpen != nil {
		mmOpen.mock.t.Fatalf("Inspect function is already set for PlanMock.Open")
	}

	mmOpen.mock.inspectFuncOpen = f

	return mmOpen
}

// Return sets up results that will be returned by Plan.Open
func (mmOpen *mPlanMockOpen) Return(s1 scan.Scan, err error) *PlanMock {
	if mmOpen.mock.funcOpen != nil {
		mmOpen.mock.t.Fatalf("PlanMock.Open mock is already set by Set")
	}

	if mmOpen.defaultExpectation == nil {
		mmOpen.defaultExpectation = &PlanMockOpenExpectation{mock: mmOpen.mock}
	}
	mmOpen.defaultExpectation.results = &PlanMockOpenResults{s1, err}
	return mmOpen.mock
}

//Set uses given function f to mock the Plan.Open method
func (mmOpen *mPlanMockOpen) Set(f func() (s1 scan.Scan, err error)) *PlanMock {
	if mmOpen.defaultExpectation != nil {
		mmOpen.mock.t.Fatalf("Default expectation is already set for the Plan.Open method")
	}

	if len(mmOpen.expectations) > 0 {
		mmOpen.mock.t.Fatalf("Some expectations are already set for the Plan.Open method")
	}

	mmOpen.mock.funcOpen = f
	return mmOpen.mock
}

// Open implements Plan
func (mmOpen *PlanMock) Open() (s1 scan.Scan, err error) {
	mm_atomic.AddUint64(&mmOpen.beforeOpenCounter, 1)
	defer mm_atomic.AddUint64(&mmOpen.afterOpenCounter, 1)

	if mmOpen.inspectFuncOpen != nil {
		mmOpen.inspectFuncOpen()
	}

	if mmOpen.OpenMock.defaultExpectation != nil {
		mm_atomic.AddUint64(&mmOpen.OpenMock.defaultExpectation.Counter, 1)

		mm_results := mmOpen.OpenMock.defaultExpectation.results
		if mm_results == nil {
			mmOpen.t.Fatal("No results are set for the PlanMock.Open")
		}
		return (*mm_results).s1, (*mm_results).err
	}
	if mmOpen.funcOpen != nil {
		return mmOpen.funcOpen()
	}
	mmOpen.t.Fatalf("Unexpected call to PlanMock.Open.")
	return
}

// OpenAfterCounter returns a count of finished PlanMock.Open invocations
func (mmOpen *PlanMock) OpenAfterCounter() uint64 {
	return mm_atomic.LoadUint64(&mmOpen.afterOpenCounter)
}

// OpenBeforeCounter returns a count of PlanMock.Open invocations
func (mmOpen *PlanMock) OpenBeforeCounter() uint64 {
	return mm_atomic.LoadUint64(&mmOpen.beforeOpenCounter)
}

// MinimockOpenDone returns true if the count of the Open invocations corresponds
// the number of defined expectations
func (m *PlanMock) MinimockOpenDone() bool {
	for _, e := range m.OpenMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			return false
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.OpenMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterOpenCounter) < 1 {
		return false
	}
	// if func was set then invocations count should be greater than zero
	if m.funcOpen != nil && mm_atomic.LoadUint64(&m.afterOpenCounter) < 1 {
		return false
	}
	return true
}

// MinimockOpenInspect logs each unmet expectation
func (m *PlanMock) MinimockOpenInspect() {
	for _, e := range m.OpenMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			m.t.Error("Expected call to PlanMock.Open")
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.OpenMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterOpenCounter) < 1 {
		m.t.Error("Expected call to PlanMock.Open")
	}
	// if func was set then invocations count should be greater than zero
	if m.funcOpen != nil && mm_atomic.LoadUint64(&m.afterOpenCounter) < 1 {
		m.t.Error("Expected call to PlanMock.Open")
	}
}

type mPlanMockRecords struct {
	mock               *PlanMock
	defaultExpectation *PlanMockRecordsExpectation
	expectations       []*PlanMockRecordsExpectation
}

// PlanMockRecordsExpectation specifies expectation struct of the Plan.Records
type PlanMockRecordsExpectation struct {
	mock *PlanMock

	results *PlanMockRecordsResults
	Counter uint64
}

// PlanMockRecordsResults contains results of the Plan.Records
type PlanMockRecordsResults struct {
	i1 int64
}

// Expect sets up expected params for Plan.Records
func (mmRecords *mPlanMockRecords) Expect() *mPlanMockRecords {
	if mmRecords.mock.funcRecords != nil {
		mmRecords.mock.t.Fatalf("PlanMock.Records mock is already set by Set")
	}

	if mmRecords.defaultExpectation == nil {
		mmRecords.defaultExpectation = &PlanMockRecordsExpectation{}
	}

	return mmRecords
}

// Inspect accepts an inspector function that has same arguments as the Plan.Records
func (mmRecords *mPlanMockRecords) Inspect(f func()) *mPlanMockRecords {
	if mmRecords.mock.inspectFuncRecords != nil {
		mmRecords.mock.t.Fatalf("Inspect function is already set for PlanMock.Records")
	}

	mmRecords.mock.inspectFuncRecords = f

	return mmRecords
}

// Return sets up results that will be returned by Plan.Records
func (mmRecords *mPlanMockRecords) Return(i1 int64) *PlanMock {
	if mmRecords.mock.funcRecords != nil {
		mmRecords.mock.t.Fatalf("PlanMock.Records mock is already set by Set")
	}

	if mmRecords.defaultExpectation == nil {
		mmRecords.defaultExpectation = &PlanMockRecordsExpectation{mock: mmRecords.mock}
	}
	mmRecords.defaultExpectation.results = &PlanMockRecordsResults{i1}
	return mmRecords.mock
}

//Set uses given function f to mock the Plan.Records method
func (mmRecords *mPlanMockRecords) Set(f func() (i1 int64)) *PlanMock {
	if mmRecords.defaultExpectation != nil {
		mmRecords.mock.t.Fatalf("Default expectation is already set for the Plan.Records method")
	}

	if len(mmRecords.expectations) > 0 {
		mmRecords.mock.t.Fatalf("Some expectations are already set for the Plan.Records method")
	}

	mmRecords.mock.funcRecords = f
	return mmRecords.mock
}

// Records implements Plan
func (mmRecords *PlanMock) Records() (i1 int64) {
	mm_atomic.AddUint64(&mmRecords.beforeRecordsCounter, 1)
	defer mm_atomic.AddUint64(&mmRecords.afterRecordsCounter, 1)

	if mmRecords.inspectFuncRecords != nil {
		mmRecords.inspectFuncRecords()
	}

	if mmRecords.RecordsMock.defaultExpectation != nil {
		mm_atomic.AddUint64(&mmRecords.RecordsMock.defaultExpectation.Counter, 1)

		mm_results := mmRecords.RecordsMock.defaultExpectation.results
		if mm_results == nil {
			mmRecords.t.Fatal("No results are set for the PlanMock.Records")
		}
		return (*mm_results).i1
	}
	if mmRecords.funcRecords != nil {
		return mmRecords.funcRecords()
	}
	mmRecords.t.Fatalf("Unexpected call to PlanMock.Records.")
	return
}

// RecordsAfterCounter returns a count of finished PlanMock.Records invocations
func (mmRecords *PlanMock) RecordsAfterCounter() uint64 {
	return mm_atomic.LoadUint64(&mmRecords.afterRecordsCounter)
}

// RecordsBeforeCounter returns a count of PlanMock.Records invocations
func (mmRecords *PlanMock) RecordsBeforeCounter() uint64 {
	return mm_atomic.LoadUint64(&mmRecords.beforeRecordsCounter)
}

// MinimockRecordsDone returns true if the count of the Records invocations corresponds
// the number of defined expectations
func (m *PlanMock) MinimockRecordsDone() bool {
	for _, e := range m.RecordsMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			return false
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.RecordsMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterRecordsCounter) < 1 {
		return false
	}
	// if func was set then invocations count should be greater than zero
	if m.funcRecords != nil && mm_atomic.LoadUint64(&m.afterRecordsCounter) < 1 {
		return false
	}
	return true
}

// MinimockRecordsInspect logs each unmet expectation
func (m *PlanMock) MinimockRecordsInspect() {
	for _, e := range m.RecordsMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			m.t.Error("Expected call to PlanMock.Records")
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.RecordsMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterRecordsCounter) < 1 {
		m.t.Error("Expected call to PlanMock.Records")
	}
	// if func was set then invocations count should be greater than zero
	if m.funcRecords != nil && mm_atomic.LoadUint64(&m.afterRecordsCounter) < 1 {
		m.t.Error("Expected call to PlanMock.Records")
	}
}

type mPlanMockSchema struct {
	mock               *PlanMock
	defaultExpectation *PlanMockSchemaExpectation
	expectations       []*PlanMockSchemaExpectation
}

// PlanMockSchemaExpectation specifies expectation struct of the Plan.Schema
type PlanMockSchemaExpectation struct {
	mock *PlanMock

	results *PlanMockSchemaResults
	Counter uint64
}

// PlanMockSchemaResults contains results of the Plan.Schema
type PlanMockSchemaResults struct {
	s1 records.Schema
}

// Expect sets up expected params for Plan.Schema
func (mmSchema *mPlanMockSchema) Expect() *mPlanMockSchema {
	if mmSchema.mock.funcSchema != nil {
		mmSchema.mock.t.Fatalf("PlanMock.Schema mock is already set by Set")
	}

	if mmSchema.defaultExpectation == nil {
		mmSchema.defaultExpectation = &PlanMockSchemaExpectation{}
	}

	return mmSchema
}

// Inspect accepts an inspector function that has same arguments as the Plan.Schema
func (mmSchema *mPlanMockSchema) Inspect(f func()) *mPlanMockSchema {
	if mmSchema.mock.inspectFuncSchema != nil {
		mmSchema.mock.t.Fatalf("Inspect function is already set for PlanMock.Schema")
	}

	mmSchema.mock.inspectFuncSchema = f

	return mmSchema
}

// Return sets up results that will be returned by Plan.Schema
func (mmSchema *mPlanMockSchema) Return(s1 records.Schema) *PlanMock {
	if mmSchema.mock.funcSchema != nil {
		mmSchema.mock.t.Fatalf("PlanMock.Schema mock is already set by Set")
	}

	if mmSchema.defaultExpectation == nil {
		mmSchema.defaultExpectation = &PlanMockSchemaExpectation{mock: mmSchema.mock}
	}
	mmSchema.defaultExpectation.results = &PlanMockSchemaResults{s1}
	return mmSchema.mock
}

//Set uses given function f to mock the Plan.Schema method
func (mmSchema *mPlanMockSchema) Set(f func() (s1 records.Schema)) *PlanMock {
	if mmSchema.defaultExpectation != nil {
		mmSchema.mock.t.Fatalf("Default expectation is already set for the Plan.Schema method")
	}

	if len(mmSchema.expectations) > 0 {
		mmSchema.mock.t.Fatalf("Some expectations are already set for the Plan.Schema method")
	}

	mmSchema.mock.funcSchema = f
	return mmSchema.mock
}

// Schema implements Plan
func (mmSchema *PlanMock) Schema() (s1 records.Schema) {
	mm_atomic.AddUint64(&mmSchema.beforeSchemaCounter, 1)
	defer mm_atomic.AddUint64(&mmSchema.afterSchemaCounter, 1)

	if mmSchema.inspectFuncSchema != nil {
		mmSchema.inspectFuncSchema()
	}

	if mmSchema.SchemaMock.defaultExpectation != nil {
		mm_atomic.AddUint64(&mmSchema.SchemaMock.defaultExpectation.Counter, 1)

		mm_results := mmSchema.SchemaMock.defaultExpectation.results
		if mm_results == nil {
			mmSchema.t.Fatal("No results are set for the PlanMock.Schema")
		}
		return (*mm_results).s1
	}
	if mmSchema.funcSchema != nil {
		return mmSchema.funcSchema()
	}
	mmSchema.t.Fatalf("Unexpected call to PlanMock.Schema.")
	return
}

// SchemaAfterCounter returns a count of finished PlanMock.Schema invocations
func (mmSchema *PlanMock) SchemaAfterCounter() uint64 {
	return mm_atomic.LoadUint64(&mmSchema.afterSchemaCounter)
}

// SchemaBeforeCounter returns a count of PlanMock.Schema invocations
func (mmSchema *PlanMock) SchemaBeforeCounter() uint64 {
	return mm_atomic.LoadUint64(&mmSchema.beforeSchemaCounter)
}

// MinimockSchemaDone returns true if the count of the Schema invocations corresponds
// the number of defined expectations
func (m *PlanMock) MinimockSchemaDone() bool {
	for _, e := range m.SchemaMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			return false
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.SchemaMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterSchemaCounter) < 1 {
		return false
	}
	// if func was set then invocations count should be greater than zero
	if m.funcSchema != nil && mm_atomic.LoadUint64(&m.afterSchemaCounter) < 1 {
		return false
	}
	return true
}

// MinimockSchemaInspect logs each unmet expectation
func (m *PlanMock) MinimockSchemaInspect() {
	for _, e := range m.SchemaMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			m.t.Error("Expected call to PlanMock.Schema")
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.SchemaMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterSchemaCounter) < 1 {
		m.t.Error("Expected call to PlanMock.Schema")
	}
	// if func was set then invocations count should be greater than zero
	if m.funcSchema != nil && mm_atomic.LoadUint64(&m.afterSchemaCounter) < 1 {
		m.t.Error("Expected call to PlanMock.Schema")
	}
}

type mPlanMockString struct {
	mock               *PlanMock
	defaultExpectation *PlanMockStringExpectation
	expectations       []*PlanMockStringExpectation
}

// PlanMockStringExpectation specifies expectation struct of the Plan.String
type PlanMockStringExpectation struct {
	mock *PlanMock

	results *PlanMockStringResults
	Counter uint64
}

// PlanMockStringResults contains results of the Plan.String
type PlanMockStringResults struct {
	s1 string
}

// Expect sets up expected params for Plan.String
func (mmString *mPlanMockString) Expect() *mPlanMockString {
	if mmString.mock.funcString != nil {
		mmString.mock.t.Fatalf("PlanMock.String mock is already set by Set")
	}

	if mmString.defaultExpectation == nil {
		mmString.defaultExpectation = &PlanMockStringExpectation{}
	}

	return mmString
}

// Inspect accepts an inspector function that has same arguments as the Plan.String
func (mmString *mPlanMockString) Inspect(f func()) *mPlanMockString {
	if mmString.mock.inspectFuncString != nil {
		mmString.mock.t.Fatalf("Inspect function is already set for PlanMock.String")
	}

	mmString.mock.inspectFuncString = f

	return mmString
}

// Return sets up results that will be returned by Plan.String
func (mmString *mPlanMockString) Return(s1 string) *PlanMock {
	if mmString.mock.funcString != nil {
		mmString.mock.t.Fatalf("PlanMock.String mock is already set by Set")
	}

	if mmString.defaultExpectation == nil {
		mmString.defaultExpectation = &PlanMockStringExpectation{mock: mmString.mock}
	}
	mmString.defaultExpectation.results = &PlanMockStringResults{s1}
	return mmString.mock
}

//Set uses given function f to mock the Plan.String method
func (mmString *mPlanMockString) Set(f func() (s1 string)) *PlanMock {
	if mmString.defaultExpectation != nil {
		mmString.mock.t.Fatalf("Default expectation is already set for the Plan.String method")
	}

	if len(mmString.expectations) > 0 {
		mmString.mock.t.Fatalf("Some expectations are already set for the Plan.String method")
	}

	mmString.mock.funcString = f
	return mmString.mock
}

// String implements Plan
func (mmString *PlanMock) String() (s1 string) {
	mm_atomic.AddUint64(&mmString.beforeStringCounter, 1)
	defer mm_atomic.AddUint64(&mmString.afterStringCounter, 1)

	if mmString.inspectFuncString != nil {
		mmString.inspectFuncString()
	}

	if mmString.StringMock.defaultExpectation != nil {
		mm_atomic.AddUint64(&mmString.StringMock.defaultExpectation.Counter, 1)

		mm_results := mmString.StringMock.defaultExpectation.results
		if mm_results == nil {
			mmString.t.Fatal("No results are set for the PlanMock.String")
		}
		return (*mm_results).s1
	}
	if mmString.funcString != nil {
		return mmString.funcString()
	}
	mmString.t.Fatalf("Unexpected call to PlanMock.String.")
	return
}

// StringAfterCounter returns a count of finished PlanMock.String invocations
func (mmString *PlanMock) StringAfterCounter() uint64 {
	return mm_atomic.LoadUint64(&mmString.afterStringCounter)
}

// StringBeforeCounter returns a count of PlanMock.String invocations
func (mmString *PlanMock) StringBeforeCounter() uint64 {
	return mm_atomic.LoadUint64(&mmString.beforeStringCounter)
}

// MinimockStringDone returns true if the count of the String invocations corresponds
// the number of defined expectations
func (m *PlanMock) MinimockStringDone() bool {
	for _, e := range m.StringMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			return false
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.StringMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterStringCounter) < 1 {
		return false
	}
	// if func was set then invocations count should be greater than zero
	if m.funcString != nil && mm_atomic.LoadUint64(&m.afterStringCounter) < 1 {
		return false
	}
	return true
}

// MinimockStringInspect logs each unmet expectation
func (m *PlanMock) MinimockStringInspect() {
	for _, e := range m.StringMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			m.t.Error("Expected call to PlanMock.String")
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.StringMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterStringCounter) < 1 {
		m.t.Error("Expected call to PlanMock.String")
	}
	// if func was set then invocations count should be greater than zero
	if m.funcString != nil && mm_atomic.LoadUint64(&m.afterStringCounter) < 1 {
		m.t.Error("Expected call to PlanMock.String")
	}
}

// MinimockFinish checks that all mocked methods have been called the expected number of times
func (m *PlanMock) MinimockFinish() {
	if !m.minimockDone() {
		m.MinimockBlocksAccessedInspect()

		m.MinimockDistinctValuesInspect()

		m.MinimockOpenInspect()

		m.MinimockRecordsInspect()

		m.MinimockSchemaInspect()

		m.MinimockStringInspect()
		m.t.FailNow()
	}
}

// MinimockWait waits for all mocked methods to be called the expected number of times
func (m *PlanMock) MinimockWait(timeout mm_time.Duration) {
	timeoutCh := mm_time.After(timeout)
	for {
		if m.minimockDone() {
			return
		}
		select {
		case <-timeoutCh:
			m.MinimockFinish()
			return
		case <-mm_time.After(10 * mm_time.Millisecond):
		}
	}
}

func (m *PlanMock) minimockDone() bool {
	done := true
	return done &&
		m.MinimockBlocksAccessedDone() &&
		m.MinimockDistinctValuesDone() &&
		m.MinimockOpenDone() &&
		m.MinimockRecordsDone() &&
		m.MinimockSchemaDone() &&
		m.MinimockStringDone()
}
