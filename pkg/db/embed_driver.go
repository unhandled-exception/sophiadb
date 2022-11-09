// Встроенный драйвер для SophiaDB
//
// Строка соединения со встроенной базой:
// path_to_db_folder?block_size=4096&transaction_lock_timeout=3s
//
// Допустимые параметры:
//   block_size (uint32) — размер блока в байтах
//   buffers_pool_len (int) — длина пула буферов. Общий размер в памяти buffers_poll_size*block_size
//   log_file_name (string) — имя файла для wal-лога
//   pin_lock_timeout (duration) — таймаут для пина буферов
//   transaction_lock_timeout (duration) - таймаут ожидания взятия блокировки транзакцией
//
// duration format:
// ParseDuration parses a duration string. A duration string is a possibly signed sequence of
// decimal numbers, each with optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m".
// Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"sync"

	"github.com/pkg/errors"

	"github.com/unhandled-exception/sophiadb/internal/pkg/planner"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
	"github.com/unhandled-exception/sophiadb/internal/pkg/utils"
)

const EmbedDriverName = "sophiadb:embed"

func init() {
	sql.Register(EmbedDriverName, NewEmbedDriver())
}

var (
	_ driver.Driver        = &EmbedDriver{}
	_ driver.DriverContext = &EmbedDriver{}
	_ driver.Connector     = &connector{}

	_ driver.Pinger          = &EmbedConn{}
	_ driver.SessionResetter = &EmbedConn{}
	_ driver.Validator       = &EmbedConn{}
	_ driver.Pinger          = &EmbedConn{}
)

type EmbedDriver struct {
	databases map[string]*Database

	mu sync.Mutex
}

func NewEmbedDriver() *EmbedDriver {
	d := &EmbedDriver{
		databases: make(map[string]*Database),
	}

	return d
}

func (d *EmbedDriver) OpenConnector(dsn string) (driver.Connector, error) {
	c := connector{
		dsn:    dsn,
		driver: d,
	}

	return c, nil
}

func (d *EmbedDriver) Open(dsn string) (driver.Conn, error) {
	var err error

	parsedDSN, err := parseEmbedDSN(dsn)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	db, ok := d.databases[parsedDSN.DataDir]
	if !ok {
		db, err = d.newDB(parsedDSN)
		if err != nil {
			return nil, err
		}

		d.databases[parsedDSN.DataDir] = db
	}

	return NewEmbedConn(db)
}

func (d *EmbedDriver) Close() error {
	errs := []error{}

	for _, db := range d.databases {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.New(utils.JoinErrors(errs, ", "))
	}

	return nil
}

func (d *EmbedDriver) newDB(dsn embedDSN) (*Database, error) {
	return NewDatabase(
		dsn.DataDir,
		WithBlockSize(dsn.BlockSize),
		WithLogFileName(dsn.LogFileName),
		WithBuffersPoolLen(dsn.BuffersPoolLen),
		WithPinLockTimeout(dsn.PinLockTimeout),
		WithTransactionLockTimeout(dsn.TransactionLockTimeout),
	)
}

type EmbedConn struct {
	db  *Database
	trx *transaction.Transaction

	inTrx bool
}

func NewEmbedConn(db *Database) (*EmbedConn, error) {
	c := &EmbedConn{
		db: db,
	}

	trx, err := db.Transaction()
	if err != nil {
		return nil, err
	}

	c.trx = trx

	return c, nil
}

func (e *EmbedConn) DB() *Database {
	return e.db
}

func (e *EmbedConn) TRX() *transaction.Transaction {
	return e.trx
}

func (e *EmbedConn) Ping(ctx context.Context) error {
	return nil
}

func (e *EmbedConn) IsValid() bool {
	return true
}

func (e *EmbedConn) PrepareContext(_ context.Context, statement string) (driver.Stmt, error) {
	return e.Prepare(statement)
}

func (e *EmbedConn) Prepare(statement string) (driver.Stmt, error) {
	return &embedStmt{
		conn:      e,
		statement: statement,
		planner:   e.db.Planner(),
	}, nil
}

func (e *EmbedConn) Close() error {
	return e.trx.Rollback()
}

func (e *EmbedConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if e.inTrx {
		return nil, ErrTransactionAlreadyStarted
	}

	e.inTrx = true

	return e, nil
}

func (e *EmbedConn) Begin() (driver.Tx, error) {
	return nil, errors.New("Begin is not implemented, use BeginTx instead")
}

func (e *EmbedConn) Commit() error {
	err := e.trx.Commit()
	if err != nil {
		return err
	}

	e.trx, err = e.db.Transaction()
	if err != nil {
		return err
	}

	e.inTrx = false

	return nil
}

func (e *EmbedConn) Rollback() error {
	err := e.trx.Rollback()
	if err != nil {
		return err
	}

	e.trx, err = e.db.Transaction()
	if err != nil {
		return err
	}

	e.inTrx = false

	return nil
}

func (e *EmbedConn) ResetSession(ctx context.Context) error {
	return e.Rollback()
}

type embedStmt struct {
	statement string

	conn    *EmbedConn
	planner planner.Planner
}

func (s embedStmt) Close() error {
	return nil
}

func (s embedStmt) NumInput() int {
	return -1
}

func (s embedStmt) ExecContext(_ context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.exec(s.statement, args)
}

func (s embedStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("Exec is not implemented, use ExecContext instead")
}

func (s embedStmt) exec(statement string, args []driver.NamedValue) (driver.Result, error) {
	statement, err := applyPlaceholders(statement, args)
	if err != nil {
		return nil, err
	}

	rows, err := s.planner.ExecuteCommand(statement, s.conn.TRX())
	if err != nil {
		return nil, err
	}

	if !s.conn.inTrx {
		err = s.conn.Commit()
		if err != nil {
			return nil, err
		}
	}

	return stmtResult{rows: rows}, nil
}

func (s embedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.query(s.statement, args)
}

func (s embedStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Query is not implemented, use QueryContext instead")
}

func (s embedStmt) query(query string, args []driver.NamedValue) (driver.Rows, error) {
	query, err := applyPlaceholders(query, args)
	if err != nil {
		return nil, err
	}

	plan, err := s.planner.CreateQueryPlan(query, s.conn.TRX())
	if err != nil {
		return nil, err
	}

	scan, err := plan.Open()
	if err != nil {
		return nil, err
	}

	return embedRows{
		plan: plan,
		scan: scan,
	}, nil
}

type embedRows struct {
	plan planner.Plan
	scan scan.Scan
}

func (r embedRows) Columns() []string {
	return r.plan.Schema().Fields()
}

func (r embedRows) Close() error {
	r.scan.Close()

	return nil
}

func (r embedRows) Next(dest []driver.Value) error {
	ok, err := r.scan.Next()
	if err != nil {
		return err
	}

	if !ok {
		return io.EOF
	}

	schema := r.plan.Schema()

	var val any

	for i, field := range schema.Fields() {
		switch schema.Type(field) { //nolint:exhaustive
		case records.Int64Field:
			val, err = r.scan.GetInt64(field)
		case records.Int8Field:
			val, err = r.scan.GetInt8(field)
		case records.StringField:
			val, err = r.scan.GetString(field)
		default:
			err = scan.ErrUnknownFieldType
		}

		if err != nil {
			return err
		}

		dest[i] = val
	}

	return nil
}
