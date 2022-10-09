package parse

import (
	"strings"

	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type FieldsList []string

func (f FieldsList) String() string {
	return strings.Join(f, ", ")
}

type TablesList []string

func (t TablesList) String() string {
	return strings.Join(t, ", ")
}

type ValuesList []scan.Constant

func (c ValuesList) String() string {
	l := make([]string, len(c))

	for i := 0; i < len(c); i++ {
		l[i] = c[i].String()
	}

	return strings.Join(l, ", ")
}
