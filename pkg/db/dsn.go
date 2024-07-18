package db

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const (
	optBlockSize              = "block_size"
	optLogFilename            = "log_file_name"
	optBuffersPoolLen         = "buffers_pool_len"
	optPinLockTimeout         = "pin_lock_timeout"
	optTransactionLockTimeout = "transaction_lock_timeout"
)

type embedDSN struct {
	DataDir                string
	LogFileName            string
	BuffersPoolLen         int
	BlockSize              uint32
	PinLockTimeout         time.Duration
	TransactionLockTimeout time.Duration
}

func parseEmbedDSN(dsn string) (embedDSN, error) {
	d := embedDSN{
		LogFileName:            DefaultLogFilename,
		BuffersPoolLen:         DefaultBuffersPoolLen,
		BlockSize:              DefaultBlockSize,
		PinLockTimeout:         DefaultPinLockTimeout,
		TransactionLockTimeout: DefaultTransactionLockTimeout,
	}

	// Вручную разбиваем строку на путь и параметры,
	// потомучто url.Parse не умеет работать с путями Windows
	parts := strings.SplitN(dsn, "?", 2) //nolint:mnd
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return d, errors.WithMessage(ErrBadDSN, "empty path")
	}

	d.DataDir = parts[0]

	if len(parts) == 1 {
		return d, nil
	}

	opts, err := url.ParseQuery(parts[1])
	if err != nil {
		return d, errors.WithMessage(ErrBadDSN, err.Error())
	}

	for name, values := range opts {
		switch name {
		case optBlockSize:
			v, err1 := strconv.ParseUint(values[0], 10, 32) //nolint:mnd
			if err1 != nil {
				return d, errors.WithMessagef(ErrBadDSN, "bad uint32 value: %s", err1)
			}

			d.BlockSize = uint32(v)
		case optLogFilename:
			d.LogFileName = values[0]
		case optBuffersPoolLen:
			v, err1 := strconv.ParseInt(values[0], 10, 32) //nolint:mnd
			if err1 != nil {
				return d, errors.WithMessagef(ErrBadDSN, "bad int value: %s", err1)
			}

			d.BuffersPoolLen = int(v)
		case optPinLockTimeout:
			v, err1 := time.ParseDuration(values[0])
			if err1 != nil {
				return d, errors.WithMessagef(ErrBadDSN, "bad duration value: %s", err1)
			}

			d.PinLockTimeout = v
		case optTransactionLockTimeout:
			v, err1 := time.ParseDuration(values[0])
			if err1 != nil {
				return d, errors.WithMessagef(ErrBadDSN, "bad duration value: %s", err1)
			}

			d.TransactionLockTimeout = v
		default:
			return d, errors.WithMessagef(ErrBadDSN, "unknown key: %s", name)
		}
	}

	return d, nil
}
