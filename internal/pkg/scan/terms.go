package scan

import (
	"math"

	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
)

type Term interface {
	IsSatisfied(Scan) (bool, error)
	AppliesTo(records.Schema) bool
	ReductionFactor(Plan) (int64, bool)
	EquatesWithConstant(string) (Constant, bool)
	EquatesWithField(string) (string, bool)
	String() string
}

type EqualTerm struct {
	lhs Expression
	rhs Expression
}

func NewEqualTerm(lhs Expression, rhs Expression) EqualTerm {
	return EqualTerm{
		lhs: lhs,
		rhs: rhs,
	}
}

func (et EqualTerm) IsSatisfied(s Scan) (bool, error) {
	lval, err := et.lhs.Evaluate(s)
	if err != nil {
		return false, err
	}

	rval, err := et.rhs.Evaluate(s)
	if err != nil {
		return false, err
	}

	return rval.CompareTo(lval) == CompEqual, nil
}

func (et EqualTerm) String() string {
	return et.lhs.String() + ` = ` + et.rhs.String()
}

func (et EqualTerm) AppliesTo(s records.Schema) bool {
	return et.lhs.AppliesTo(s) && et.rhs.AppliesTo(s)
}

//nolint:forcetypeassert
func (et EqualTerm) ReductionFactor(p Plan) (int64, bool) {
	if et.lhs.IsFieldName() && et.rhs.IsFieldName() {
		lrf, _ := et.lhs.ReductionFactor(p)
		rrf, _ := et.rhs.ReductionFactor(p)

		return max(lrf, rrf), true
	}

	if et.lhs.IsFieldName() {
		return et.lhs.ReductionFactor(p)
	}

	if et.rhs.IsFieldName() {
		return et.rhs.ReductionFactor(p)
	}

	lv, _ := et.lhs.Value()
	rv, _ := et.rhs.Value()

	if lv.(Constant).CompareTo(rv.(Constant)) == CompEqual {
		return 1, true
	}

	return math.MaxInt64, true
}

//nolint:forcetypeassert
func (et EqualTerm) EquatesWithConstant(fieldName string) (Constant, bool) {
	lv, _ := et.lhs.Value()
	rv, _ := et.rhs.Value()

	if et.lhs.IsFieldName() && !et.rhs.IsFieldName() && lv.(string) == fieldName {
		return rv.(Constant), true
	}

	if et.rhs.IsFieldName() && !et.lhs.IsFieldName() && rv.(string) == fieldName {
		return lv.(Constant), true
	}

	return nil, false
}

//nolint:forcetypeassert
func (et EqualTerm) EquatesWithField(fieldName string) (string, bool) {
	lv, _ := et.lhs.Value()
	rv, _ := et.rhs.Value()

	if et.lhs.IsFieldName() && et.rhs.IsFieldName() && lv.(string) == fieldName {
		return rv.(string), true
	}

	if et.lhs.IsFieldName() && et.rhs.IsFieldName() && rv.(string) == fieldName {
		return lv.(string), true
	}

	return "", false
}
