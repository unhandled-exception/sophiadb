package parse

import "github.com/pkg/errors"

var (
	ErrEOF              = errors.New("there are no more tokens")
	ErrBadSyntax        = errors.New("bad syntax")
	ErrUnmatchedKeyword = errors.Wrap(ErrBadSyntax, "unmatched keyword")
	ErrUnmatchedDelim   = errors.Wrap(ErrBadSyntax, "unmatched delimiter")
	ErrInvalidStatement = errors.New("invalid statement")
)
