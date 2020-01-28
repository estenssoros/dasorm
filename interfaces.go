package dasorm

import (
	"context"
	"database/sql"
)

type dialect interface {
	Name() string
	TranslateSQL(string) string
	Create(*Connection, *Model) error
	CreateUpdate(*Connection, *Model) error
	CreateMany(*Connection, *Model) error
	CreateManyTemp(*Connection, *Model) error
	CreateManyUpdate(*Connection, *Model) error
	Update(*Connection, *Model) error
	Destroy(*Connection, *Model) error
	DestroyMany(*Connection, *Model) error
	SelectOne(*Connection, *Model, Query) error
	SelectMany(*Connection, *Model, Query) error
	SQLView(*Connection, *Model, map[string]string) error
}

// DBInterface implements all necessary operations for dasorm
type DBInterface interface {
	Close() error
	Ping() error
	Query(string, ...interface{}) (*sql.Rows, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) *sql.Row
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
	Exec(string, ...interface{}) (sql.Result, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	Select(interface{}, string, ...interface{}) error
	Get(interface{}, string, ...interface{}) error
	NamedExec(string, interface{}) (sql.Result, error)
}
