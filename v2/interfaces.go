package dasorm

import (
	"context"
	"database/sql"
)

// DBInterface interface for our database operations
type DBInterface interface {
	Debug() bool
	SetDebug(bool)
	Get(interface{}, string, ...interface{}) error
	Select(interface{}, string, ...interface{}) error
	NamedExec(string, interface{}) (sql.Result, error)
	Close() error
	Ping() error
	Query(string, ...interface{}) (*sql.Rows, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) *sql.Row
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
	Exec(string, ...interface{}) (sql.Result, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}
