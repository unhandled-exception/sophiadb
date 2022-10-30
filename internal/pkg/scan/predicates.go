package scan

import (
	"strings"

	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
)

type Predicate interface {
	String() string
	Terms() []Term
	ConjoinWith(Predicate)
	IsSatisfied(Scan) (bool, error)
	ReductionFactor(Plan) (int64, bool)
	SelectSubPred(records.Schema) Predicate
	JoinSubPred(records.Schema, records.Schema) Predicate
	EquatesWithConstant(string) (Constant, bool)
	EquatesWithField(string) (string, bool)
}

type AndPredicate struct {
	terms []Term
}

func NewAndPredicate(terms ...Term) *AndPredicate {
	l := len(terms)
	p := &AndPredicate{
		terms: make([]Term, 0, l),
	}

	if l > 0 {
		p.terms = append(p.terms, terms...)
	}

	return p
}

func (a *AndPredicate) String() string {
	if len(a.terms) == 0 {
		return ""
	}

	ts := make([]string, len(a.terms))

	for i, term := range a.terms {
		ts[i] = term.String()
	}

	return strings.Join(ts, " and ")
}

func (a *AndPredicate) Terms() []Term {
	return a.terms
}

func (a *AndPredicate) ConjoinWith(predicate Predicate) {
	if predicate != nil {
		a.terms = append(a.terms, predicate.Terms()...)
	}
}

func (a *AndPredicate) IsSatisfied(s Scan) (bool, error) {
	for _, term := range a.terms {
		ok, err := term.IsSatisfied(s)
		if err != nil {
			return false, err
		}

		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func (a *AndPredicate) ReductionFactor(p Plan) (int64, bool) {
	var rf int64 = 1

	for _, term := range a.terms {
		trf, ok := term.ReductionFactor(p)
		if ok {
			rf *= trf
		}
	}

	return rf, true
}

func (a *AndPredicate) SelectSubPred(s records.Schema) Predicate {
	sp := NewAndPredicate()

	for _, term := range a.terms {
		if term.AppliesTo(s) {
			sp.terms = append(sp.terms, term)
		}
	}

	return sp
}

// JoinSubPred возвращает предикат с условиями, которые применимы к сумме схем s1 и s2, но не к каждой схеме отдельно
func (a *AndPredicate) JoinSubPred(s1 records.Schema, s2 records.Schema) Predicate {
	if len(a.terms) == 0 {
		return nil
	}

	schema := records.NewSchema()
	schema.AddAll(s1)
	schema.AddAll(s2)

	p := NewAndPredicate()

	for _, term := range a.terms {
		if !term.AppliesTo(s1) && !term.AppliesTo(s2) && term.AppliesTo(schema) {
			p.terms = append(p.terms, term)
		}
	}

	if len(p.terms) == 0 {
		return nil
	}

	return p
}

func (a *AndPredicate) EquatesWithConstant(fieldName string) (Constant, bool) {
	for _, term := range a.terms {
		c, ok := term.EquatesWithConstant(fieldName)
		if ok {
			return c, true
		}
	}

	return nil, false
}

func (a *AndPredicate) EquatesWithField(fieldName string) (string, bool) {
	for _, term := range a.terms {
		f, ok := term.EquatesWithField(fieldName)
		if ok {
			return f, true
		}
	}

	return "", false
}
