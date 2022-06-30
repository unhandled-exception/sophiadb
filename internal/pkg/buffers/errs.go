package buffers

import "github.com/pkg/errors"

// ErrBuffers базовая ошибка buffers
var ErrBuffers = errors.New("buffers error")

// ErrFailedToAssignBlockToBuffer — ошибка при связывании буыера с блоком
var ErrFailedToAssignBlockToBuffer = errors.Wrap(ErrBuffers, "failed to assign a block to buffer")

// ErrNoAvailableBuffers — нет свободных буферов в памяти
var ErrNoAvailableBuffers = errors.Wrap(ErrBuffers, "no available buffers")
