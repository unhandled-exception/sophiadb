package scan

// Code generated by http://github.com/gojuno/minimock (dev). DO NOT EDIT.

//go:generate minimock -i github.com/unhandled-exception/sophiadb/pkg/scan.Constant -o ./constants_mock_test.go -n ConstantMock

import (
	"sync"
	mm_atomic "sync/atomic"
	mm_time "time"

	"github.com/gojuno/minimock/v3"
	"github.com/unhandled-exception/sophiadb/pkg/records"
)

// ConstantMock implements Constant
type ConstantMock struct {
	t minimock.Tester

	funcCompareTo          func(c1 Constant) (c2 CompResult)
	inspectFuncCompareTo   func(c1 Constant)
	afterCompareToCounter  uint64
	beforeCompareToCounter uint64
	CompareToMock          mConstantMockCompareTo

	funcString          func() (s1 string)
	inspectFuncString   func()
	afterStringCounter  uint64
	beforeStringCounter uint64
	StringMock          mConstantMockString

	funcType          func() (f1 records.FieldType)
	inspectFuncType   func()
	afterTypeCounter  uint64
	beforeTypeCounter uint64
	TypeMock          mConstantMockType

	funcValue          func() (p1 interface{})
	inspectFuncValue   func()
	afterValueCounter  uint64
	beforeValueCounter uint64
	ValueMock          mConstantMockValue
}

// NewConstantMock returns a mock for Constant
func NewConstantMock(t minimock.Tester) *ConstantMock {
	m := &ConstantMock{t: t}
	if controller, ok := t.(minimock.MockController); ok {
		controller.RegisterMocker(m)
	}

	m.CompareToMock = mConstantMockCompareTo{mock: m}
	m.CompareToMock.callArgs = []*ConstantMockCompareToParams{}

	m.StringMock = mConstantMockString{mock: m}

	m.TypeMock = mConstantMockType{mock: m}

	m.ValueMock = mConstantMockValue{mock: m}

	return m
}

type mConstantMockCompareTo struct {
	mock               *ConstantMock
	defaultExpectation *ConstantMockCompareToExpectation
	expectations       []*ConstantMockCompareToExpectation

	callArgs []*ConstantMockCompareToParams
	mutex    sync.RWMutex
}

// ConstantMockCompareToExpectation specifies expectation struct of the Constant.CompareTo
type ConstantMockCompareToExpectation struct {
	mock    *ConstantMock
	params  *ConstantMockCompareToParams
	results *ConstantMockCompareToResults
	Counter uint64
}

// ConstantMockCompareToParams contains parameters of the Constant.CompareTo
type ConstantMockCompareToParams struct {
	c1 Constant
}

// ConstantMockCompareToResults contains results of the Constant.CompareTo
type ConstantMockCompareToResults struct {
	c2 CompResult
}

// Expect sets up expected params for Constant.CompareTo
func (mmCompareTo *mConstantMockCompareTo) Expect(c1 Constant) *mConstantMockCompareTo {
	if mmCompareTo.mock.funcCompareTo != nil {
		mmCompareTo.mock.t.Fatalf("ConstantMock.CompareTo mock is already set by Set")
	}

	if mmCompareTo.defaultExpectation == nil {
		mmCompareTo.defaultExpectation = &ConstantMockCompareToExpectation{}
	}

	mmCompareTo.defaultExpectation.params = &ConstantMockCompareToParams{c1}
	for _, e := range mmCompareTo.expectations {
		if minimock.Equal(e.params, mmCompareTo.defaultExpectation.params) {
			mmCompareTo.mock.t.Fatalf("Expectation set by When has same params: %#v", *mmCompareTo.defaultExpectation.params)
		}
	}

	return mmCompareTo
}

// Inspect accepts an inspector function that has same arguments as the Constant.CompareTo
func (mmCompareTo *mConstantMockCompareTo) Inspect(f func(c1 Constant)) *mConstantMockCompareTo {
	if mmCompareTo.mock.inspectFuncCompareTo != nil {
		mmCompareTo.mock.t.Fatalf("Inspect function is already set for ConstantMock.CompareTo")
	}

	mmCompareTo.mock.inspectFuncCompareTo = f

	return mmCompareTo
}

// Return sets up results that will be returned by Constant.CompareTo
func (mmCompareTo *mConstantMockCompareTo) Return(c2 CompResult) *ConstantMock {
	if mmCompareTo.mock.funcCompareTo != nil {
		mmCompareTo.mock.t.Fatalf("ConstantMock.CompareTo mock is already set by Set")
	}

	if mmCompareTo.defaultExpectation == nil {
		mmCompareTo.defaultExpectation = &ConstantMockCompareToExpectation{mock: mmCompareTo.mock}
	}
	mmCompareTo.defaultExpectation.results = &ConstantMockCompareToResults{c2}
	return mmCompareTo.mock
}

//Set uses given function f to mock the Constant.CompareTo method
func (mmCompareTo *mConstantMockCompareTo) Set(f func(c1 Constant) (c2 CompResult)) *ConstantMock {
	if mmCompareTo.defaultExpectation != nil {
		mmCompareTo.mock.t.Fatalf("Default expectation is already set for the Constant.CompareTo method")
	}

	if len(mmCompareTo.expectations) > 0 {
		mmCompareTo.mock.t.Fatalf("Some expectations are already set for the Constant.CompareTo method")
	}

	mmCompareTo.mock.funcCompareTo = f
	return mmCompareTo.mock
}

// When sets expectation for the Constant.CompareTo which will trigger the result defined by the following
// Then helper
func (mmCompareTo *mConstantMockCompareTo) When(c1 Constant) *ConstantMockCompareToExpectation {
	if mmCompareTo.mock.funcCompareTo != nil {
		mmCompareTo.mock.t.Fatalf("ConstantMock.CompareTo mock is already set by Set")
	}

	expectation := &ConstantMockCompareToExpectation{
		mock:   mmCompareTo.mock,
		params: &ConstantMockCompareToParams{c1},
	}
	mmCompareTo.expectations = append(mmCompareTo.expectations, expectation)
	return expectation
}

// Then sets up Constant.CompareTo return parameters for the expectation previously defined by the When method
func (e *ConstantMockCompareToExpectation) Then(c2 CompResult) *ConstantMock {
	e.results = &ConstantMockCompareToResults{c2}
	return e.mock
}

// CompareTo implements Constant
func (mmCompareTo *ConstantMock) CompareTo(c1 Constant) (c2 CompResult) {
	mm_atomic.AddUint64(&mmCompareTo.beforeCompareToCounter, 1)
	defer mm_atomic.AddUint64(&mmCompareTo.afterCompareToCounter, 1)

	if mmCompareTo.inspectFuncCompareTo != nil {
		mmCompareTo.inspectFuncCompareTo(c1)
	}

	mm_params := &ConstantMockCompareToParams{c1}

	// Record call args
	mmCompareTo.CompareToMock.mutex.Lock()
	mmCompareTo.CompareToMock.callArgs = append(mmCompareTo.CompareToMock.callArgs, mm_params)
	mmCompareTo.CompareToMock.mutex.Unlock()

	for _, e := range mmCompareTo.CompareToMock.expectations {
		if minimock.Equal(e.params, mm_params) {
			mm_atomic.AddUint64(&e.Counter, 1)
			return e.results.c2
		}
	}

	if mmCompareTo.CompareToMock.defaultExpectation != nil {
		mm_atomic.AddUint64(&mmCompareTo.CompareToMock.defaultExpectation.Counter, 1)
		mm_want := mmCompareTo.CompareToMock.defaultExpectation.params
		mm_got := ConstantMockCompareToParams{c1}
		if mm_want != nil && !minimock.Equal(*mm_want, mm_got) {
			mmCompareTo.t.Errorf("ConstantMock.CompareTo got unexpected parameters, want: %#v, got: %#v%s\n", *mm_want, mm_got, minimock.Diff(*mm_want, mm_got))
		}

		mm_results := mmCompareTo.CompareToMock.defaultExpectation.results
		if mm_results == nil {
			mmCompareTo.t.Fatal("No results are set for the ConstantMock.CompareTo")
		}
		return (*mm_results).c2
	}
	if mmCompareTo.funcCompareTo != nil {
		return mmCompareTo.funcCompareTo(c1)
	}
	mmCompareTo.t.Fatalf("Unexpected call to ConstantMock.CompareTo. %v", c1)
	return
}

// CompareToAfterCounter returns a count of finished ConstantMock.CompareTo invocations
func (mmCompareTo *ConstantMock) CompareToAfterCounter() uint64 {
	return mm_atomic.LoadUint64(&mmCompareTo.afterCompareToCounter)
}

// CompareToBeforeCounter returns a count of ConstantMock.CompareTo invocations
func (mmCompareTo *ConstantMock) CompareToBeforeCounter() uint64 {
	return mm_atomic.LoadUint64(&mmCompareTo.beforeCompareToCounter)
}

// Calls returns a list of arguments used in each call to ConstantMock.CompareTo.
// The list is in the same order as the calls were made (i.e. recent calls have a higher index)
func (mmCompareTo *mConstantMockCompareTo) Calls() []*ConstantMockCompareToParams {
	mmCompareTo.mutex.RLock()

	argCopy := make([]*ConstantMockCompareToParams, len(mmCompareTo.callArgs))
	copy(argCopy, mmCompareTo.callArgs)

	mmCompareTo.mutex.RUnlock()

	return argCopy
}

// MinimockCompareToDone returns true if the count of the CompareTo invocations corresponds
// the number of defined expectations
func (m *ConstantMock) MinimockCompareToDone() bool {
	for _, e := range m.CompareToMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			return false
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.CompareToMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterCompareToCounter) < 1 {
		return false
	}
	// if func was set then invocations count should be greater than zero
	if m.funcCompareTo != nil && mm_atomic.LoadUint64(&m.afterCompareToCounter) < 1 {
		return false
	}
	return true
}

// MinimockCompareToInspect logs each unmet expectation
func (m *ConstantMock) MinimockCompareToInspect() {
	for _, e := range m.CompareToMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			m.t.Errorf("Expected call to ConstantMock.CompareTo with params: %#v", *e.params)
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.CompareToMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterCompareToCounter) < 1 {
		if m.CompareToMock.defaultExpectation.params == nil {
			m.t.Error("Expected call to ConstantMock.CompareTo")
		} else {
			m.t.Errorf("Expected call to ConstantMock.CompareTo with params: %#v", *m.CompareToMock.defaultExpectation.params)
		}
	}
	// if func was set then invocations count should be greater than zero
	if m.funcCompareTo != nil && mm_atomic.LoadUint64(&m.afterCompareToCounter) < 1 {
		m.t.Error("Expected call to ConstantMock.CompareTo")
	}
}

type mConstantMockString struct {
	mock               *ConstantMock
	defaultExpectation *ConstantMockStringExpectation
	expectations       []*ConstantMockStringExpectation
}

// ConstantMockStringExpectation specifies expectation struct of the Constant.String
type ConstantMockStringExpectation struct {
	mock *ConstantMock

	results *ConstantMockStringResults
	Counter uint64
}

// ConstantMockStringResults contains results of the Constant.String
type ConstantMockStringResults struct {
	s1 string
}

// Expect sets up expected params for Constant.String
func (mmString *mConstantMockString) Expect() *mConstantMockString {
	if mmString.mock.funcString != nil {
		mmString.mock.t.Fatalf("ConstantMock.String mock is already set by Set")
	}

	if mmString.defaultExpectation == nil {
		mmString.defaultExpectation = &ConstantMockStringExpectation{}
	}

	return mmString
}

// Inspect accepts an inspector function that has same arguments as the Constant.String
func (mmString *mConstantMockString) Inspect(f func()) *mConstantMockString {
	if mmString.mock.inspectFuncString != nil {
		mmString.mock.t.Fatalf("Inspect function is already set for ConstantMock.String")
	}

	mmString.mock.inspectFuncString = f

	return mmString
}

// Return sets up results that will be returned by Constant.String
func (mmString *mConstantMockString) Return(s1 string) *ConstantMock {
	if mmString.mock.funcString != nil {
		mmString.mock.t.Fatalf("ConstantMock.String mock is already set by Set")
	}

	if mmString.defaultExpectation == nil {
		mmString.defaultExpectation = &ConstantMockStringExpectation{mock: mmString.mock}
	}
	mmString.defaultExpectation.results = &ConstantMockStringResults{s1}
	return mmString.mock
}

//Set uses given function f to mock the Constant.String method
func (mmString *mConstantMockString) Set(f func() (s1 string)) *ConstantMock {
	if mmString.defaultExpectation != nil {
		mmString.mock.t.Fatalf("Default expectation is already set for the Constant.String method")
	}

	if len(mmString.expectations) > 0 {
		mmString.mock.t.Fatalf("Some expectations are already set for the Constant.String method")
	}

	mmString.mock.funcString = f
	return mmString.mock
}

// String implements Constant
func (mmString *ConstantMock) String() (s1 string) {
	mm_atomic.AddUint64(&mmString.beforeStringCounter, 1)
	defer mm_atomic.AddUint64(&mmString.afterStringCounter, 1)

	if mmString.inspectFuncString != nil {
		mmString.inspectFuncString()
	}

	if mmString.StringMock.defaultExpectation != nil {
		mm_atomic.AddUint64(&mmString.StringMock.defaultExpectation.Counter, 1)

		mm_results := mmString.StringMock.defaultExpectation.results
		if mm_results == nil {
			mmString.t.Fatal("No results are set for the ConstantMock.String")
		}
		return (*mm_results).s1
	}
	if mmString.funcString != nil {
		return mmString.funcString()
	}
	mmString.t.Fatalf("Unexpected call to ConstantMock.String.")
	return
}

// StringAfterCounter returns a count of finished ConstantMock.String invocations
func (mmString *ConstantMock) StringAfterCounter() uint64 {
	return mm_atomic.LoadUint64(&mmString.afterStringCounter)
}

// StringBeforeCounter returns a count of ConstantMock.String invocations
func (mmString *ConstantMock) StringBeforeCounter() uint64 {
	return mm_atomic.LoadUint64(&mmString.beforeStringCounter)
}

// MinimockStringDone returns true if the count of the String invocations corresponds
// the number of defined expectations
func (m *ConstantMock) MinimockStringDone() bool {
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
func (m *ConstantMock) MinimockStringInspect() {
	for _, e := range m.StringMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			m.t.Error("Expected call to ConstantMock.String")
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.StringMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterStringCounter) < 1 {
		m.t.Error("Expected call to ConstantMock.String")
	}
	// if func was set then invocations count should be greater than zero
	if m.funcString != nil && mm_atomic.LoadUint64(&m.afterStringCounter) < 1 {
		m.t.Error("Expected call to ConstantMock.String")
	}
}

type mConstantMockType struct {
	mock               *ConstantMock
	defaultExpectation *ConstantMockTypeExpectation
	expectations       []*ConstantMockTypeExpectation
}

// ConstantMockTypeExpectation specifies expectation struct of the Constant.Type
type ConstantMockTypeExpectation struct {
	mock *ConstantMock

	results *ConstantMockTypeResults
	Counter uint64
}

// ConstantMockTypeResults contains results of the Constant.Type
type ConstantMockTypeResults struct {
	f1 records.FieldType
}

// Expect sets up expected params for Constant.Type
func (mmType *mConstantMockType) Expect() *mConstantMockType {
	if mmType.mock.funcType != nil {
		mmType.mock.t.Fatalf("ConstantMock.Type mock is already set by Set")
	}

	if mmType.defaultExpectation == nil {
		mmType.defaultExpectation = &ConstantMockTypeExpectation{}
	}

	return mmType
}

// Inspect accepts an inspector function that has same arguments as the Constant.Type
func (mmType *mConstantMockType) Inspect(f func()) *mConstantMockType {
	if mmType.mock.inspectFuncType != nil {
		mmType.mock.t.Fatalf("Inspect function is already set for ConstantMock.Type")
	}

	mmType.mock.inspectFuncType = f

	return mmType
}

// Return sets up results that will be returned by Constant.Type
func (mmType *mConstantMockType) Return(f1 records.FieldType) *ConstantMock {
	if mmType.mock.funcType != nil {
		mmType.mock.t.Fatalf("ConstantMock.Type mock is already set by Set")
	}

	if mmType.defaultExpectation == nil {
		mmType.defaultExpectation = &ConstantMockTypeExpectation{mock: mmType.mock}
	}
	mmType.defaultExpectation.results = &ConstantMockTypeResults{f1}
	return mmType.mock
}

//Set uses given function f to mock the Constant.Type method
func (mmType *mConstantMockType) Set(f func() (f1 records.FieldType)) *ConstantMock {
	if mmType.defaultExpectation != nil {
		mmType.mock.t.Fatalf("Default expectation is already set for the Constant.Type method")
	}

	if len(mmType.expectations) > 0 {
		mmType.mock.t.Fatalf("Some expectations are already set for the Constant.Type method")
	}

	mmType.mock.funcType = f
	return mmType.mock
}

// Type implements Constant
func (mmType *ConstantMock) Type() (f1 records.FieldType) {
	mm_atomic.AddUint64(&mmType.beforeTypeCounter, 1)
	defer mm_atomic.AddUint64(&mmType.afterTypeCounter, 1)

	if mmType.inspectFuncType != nil {
		mmType.inspectFuncType()
	}

	if mmType.TypeMock.defaultExpectation != nil {
		mm_atomic.AddUint64(&mmType.TypeMock.defaultExpectation.Counter, 1)

		mm_results := mmType.TypeMock.defaultExpectation.results
		if mm_results == nil {
			mmType.t.Fatal("No results are set for the ConstantMock.Type")
		}
		return (*mm_results).f1
	}
	if mmType.funcType != nil {
		return mmType.funcType()
	}
	mmType.t.Fatalf("Unexpected call to ConstantMock.Type.")
	return
}

// TypeAfterCounter returns a count of finished ConstantMock.Type invocations
func (mmType *ConstantMock) TypeAfterCounter() uint64 {
	return mm_atomic.LoadUint64(&mmType.afterTypeCounter)
}

// TypeBeforeCounter returns a count of ConstantMock.Type invocations
func (mmType *ConstantMock) TypeBeforeCounter() uint64 {
	return mm_atomic.LoadUint64(&mmType.beforeTypeCounter)
}

// MinimockTypeDone returns true if the count of the Type invocations corresponds
// the number of defined expectations
func (m *ConstantMock) MinimockTypeDone() bool {
	for _, e := range m.TypeMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			return false
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.TypeMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterTypeCounter) < 1 {
		return false
	}
	// if func was set then invocations count should be greater than zero
	if m.funcType != nil && mm_atomic.LoadUint64(&m.afterTypeCounter) < 1 {
		return false
	}
	return true
}

// MinimockTypeInspect logs each unmet expectation
func (m *ConstantMock) MinimockTypeInspect() {
	for _, e := range m.TypeMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			m.t.Error("Expected call to ConstantMock.Type")
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.TypeMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterTypeCounter) < 1 {
		m.t.Error("Expected call to ConstantMock.Type")
	}
	// if func was set then invocations count should be greater than zero
	if m.funcType != nil && mm_atomic.LoadUint64(&m.afterTypeCounter) < 1 {
		m.t.Error("Expected call to ConstantMock.Type")
	}
}

type mConstantMockValue struct {
	mock               *ConstantMock
	defaultExpectation *ConstantMockValueExpectation
	expectations       []*ConstantMockValueExpectation
}

// ConstantMockValueExpectation specifies expectation struct of the Constant.Value
type ConstantMockValueExpectation struct {
	mock *ConstantMock

	results *ConstantMockValueResults
	Counter uint64
}

// ConstantMockValueResults contains results of the Constant.Value
type ConstantMockValueResults struct {
	p1 interface{}
}

// Expect sets up expected params for Constant.Value
func (mmValue *mConstantMockValue) Expect() *mConstantMockValue {
	if mmValue.mock.funcValue != nil {
		mmValue.mock.t.Fatalf("ConstantMock.Value mock is already set by Set")
	}

	if mmValue.defaultExpectation == nil {
		mmValue.defaultExpectation = &ConstantMockValueExpectation{}
	}

	return mmValue
}

// Inspect accepts an inspector function that has same arguments as the Constant.Value
func (mmValue *mConstantMockValue) Inspect(f func()) *mConstantMockValue {
	if mmValue.mock.inspectFuncValue != nil {
		mmValue.mock.t.Fatalf("Inspect function is already set for ConstantMock.Value")
	}

	mmValue.mock.inspectFuncValue = f

	return mmValue
}

// Return sets up results that will be returned by Constant.Value
func (mmValue *mConstantMockValue) Return(p1 interface{}) *ConstantMock {
	if mmValue.mock.funcValue != nil {
		mmValue.mock.t.Fatalf("ConstantMock.Value mock is already set by Set")
	}

	if mmValue.defaultExpectation == nil {
		mmValue.defaultExpectation = &ConstantMockValueExpectation{mock: mmValue.mock}
	}
	mmValue.defaultExpectation.results = &ConstantMockValueResults{p1}
	return mmValue.mock
}

//Set uses given function f to mock the Constant.Value method
func (mmValue *mConstantMockValue) Set(f func() (p1 interface{})) *ConstantMock {
	if mmValue.defaultExpectation != nil {
		mmValue.mock.t.Fatalf("Default expectation is already set for the Constant.Value method")
	}

	if len(mmValue.expectations) > 0 {
		mmValue.mock.t.Fatalf("Some expectations are already set for the Constant.Value method")
	}

	mmValue.mock.funcValue = f
	return mmValue.mock
}

// Value implements Constant
func (mmValue *ConstantMock) Value() (p1 interface{}) {
	mm_atomic.AddUint64(&mmValue.beforeValueCounter, 1)
	defer mm_atomic.AddUint64(&mmValue.afterValueCounter, 1)

	if mmValue.inspectFuncValue != nil {
		mmValue.inspectFuncValue()
	}

	if mmValue.ValueMock.defaultExpectation != nil {
		mm_atomic.AddUint64(&mmValue.ValueMock.defaultExpectation.Counter, 1)

		mm_results := mmValue.ValueMock.defaultExpectation.results
		if mm_results == nil {
			mmValue.t.Fatal("No results are set for the ConstantMock.Value")
		}
		return (*mm_results).p1
	}
	if mmValue.funcValue != nil {
		return mmValue.funcValue()
	}
	mmValue.t.Fatalf("Unexpected call to ConstantMock.Value.")
	return
}

// ValueAfterCounter returns a count of finished ConstantMock.Value invocations
func (mmValue *ConstantMock) ValueAfterCounter() uint64 {
	return mm_atomic.LoadUint64(&mmValue.afterValueCounter)
}

// ValueBeforeCounter returns a count of ConstantMock.Value invocations
func (mmValue *ConstantMock) ValueBeforeCounter() uint64 {
	return mm_atomic.LoadUint64(&mmValue.beforeValueCounter)
}

// MinimockValueDone returns true if the count of the Value invocations corresponds
// the number of defined expectations
func (m *ConstantMock) MinimockValueDone() bool {
	for _, e := range m.ValueMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			return false
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.ValueMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterValueCounter) < 1 {
		return false
	}
	// if func was set then invocations count should be greater than zero
	if m.funcValue != nil && mm_atomic.LoadUint64(&m.afterValueCounter) < 1 {
		return false
	}
	return true
}

// MinimockValueInspect logs each unmet expectation
func (m *ConstantMock) MinimockValueInspect() {
	for _, e := range m.ValueMock.expectations {
		if mm_atomic.LoadUint64(&e.Counter) < 1 {
			m.t.Error("Expected call to ConstantMock.Value")
		}
	}

	// if default expectation was set then invocations count should be greater than zero
	if m.ValueMock.defaultExpectation != nil && mm_atomic.LoadUint64(&m.afterValueCounter) < 1 {
		m.t.Error("Expected call to ConstantMock.Value")
	}
	// if func was set then invocations count should be greater than zero
	if m.funcValue != nil && mm_atomic.LoadUint64(&m.afterValueCounter) < 1 {
		m.t.Error("Expected call to ConstantMock.Value")
	}
}

// MinimockFinish checks that all mocked methods have been called the expected number of times
func (m *ConstantMock) MinimockFinish() {
	if !m.minimockDone() {
		m.MinimockCompareToInspect()

		m.MinimockStringInspect()

		m.MinimockTypeInspect()

		m.MinimockValueInspect()
		m.t.FailNow()
	}
}

// MinimockWait waits for all mocked methods to be called the expected number of times
func (m *ConstantMock) MinimockWait(timeout mm_time.Duration) {
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

func (m *ConstantMock) minimockDone() bool {
	done := true
	return done &&
		m.MinimockCompareToDone() &&
		m.MinimockStringDone() &&
		m.MinimockTypeDone() &&
		m.MinimockValueDone()
}