package scan

type Predicate interface {
	IsSatisfied(s Scan) (bool, error)
	String() string
}

type AndPredicate struct {
	terms []Term
}

func NewAdnPredicate(term Term) AndPredicate {
	p := AndPredicate{
		terms: make([]Term, 0),
	}

	p.terms = append(p.terms, term)

	return p
}

func (ap AndPredicate) IsSatisfied(s Scan) (bool, error) {
	panic("not implemented")
}

func (ap AndPredicate) String() string {
	panic("not implemented")
}
