package wal

import "github.com/pkg/errors"

// ErrWAL базовая ошибка wal
var ErrWAL = errors.New("wal error")

// ErrFailedToCreateNewManager — ошибка при создании нового менеджера
var ErrFailedToCreateNewManager = errors.Wrap(ErrWAL, "failed to create a new wal manager")

// ErrFailedToAppendNewRecord — ошибка при создании новой записи в wal-логе
var ErrFailedToAppendNewRecord = errors.Wrap(ErrWAL, "failed to add new record to wal log")

// ErrFailedToCreateNewIterator — ошибка при создании нового итератора
var ErrFailedToCreateNewIterator = errors.Wrap(ErrWAL, "failed to create a new wal iterator")
