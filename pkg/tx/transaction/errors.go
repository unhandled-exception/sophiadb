package transaction

import "github.com/pkg/errors"

var ErrTransactionFailed error = errors.New("transaction failed")
