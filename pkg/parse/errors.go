package parse

import "github.com/pkg/errors"

var (
	ErrUnmatchedKeyword = errors.New("unmatched keyword")
	ErrUnmatchedDelim   = errors.New("unmatched delimiter")
	ErrBadSyntax        = errors.New("bad syntax")
	ErrInvalidStatement = errors.New("invalid statement")
)
