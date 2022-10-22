package db

import (
	"context"
	"database/sql/driver"
	"io"
	"strings"
)

type stmtResult struct {
	rows int64
}

func (r stmtResult) LastInsertId() (int64, error) {
	return -1, nil
}

func (r stmtResult) RowsAffected() (int64, error) {
	return r.rows, nil
}

type embedDSN struct {
	Path string
}

func parseEmbedDSN(dsn string) (embedDSN, error) {
	d := embedDSN{}

	parts := strings.SplitN(dsn, "?", 1)

	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return d, ErrBadDSN
	}

	d.Path = parts[0]

	return d, nil
}

type connector struct {
	dsn    string
	driver driver.Driver
}

func (t connector) Connect(_ context.Context) (driver.Conn, error) {
	return t.driver.Open(t.dsn)
}

func (t connector) Driver() driver.Driver {
	return t.driver
}

func (t connector) Close() error {
	if d, ok := t.driver.(io.Closer); ok {
		return d.Close()
	}

	return nil
}
