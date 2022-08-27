package scan

type Term interface {
	IsSatisfied(s Scan) (bool, error)
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
	panic("not implemented")
}

func (et EqualTerm) String() string {
	panic("not implemented")
}
