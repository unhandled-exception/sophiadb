package indexes

import "github.com/pkg/errors"

var (
	ErrFailedToScanIndex = errors.New("failed to scan index")
	ErrUnknownIndexType  = errors.New("unknown index type")
)
