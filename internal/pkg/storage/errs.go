package storage

import "github.com/pkg/errors"

// ErrStorage базовая ошибка для всех ошибок стораджа
var ErrStorage error = errors.New("storage error")

// ErrFileManagerIO вызываем при ошибках ввода вывода
var ErrFileManagerIO error = errors.Wrap(ErrStorage, "file manager io error")
