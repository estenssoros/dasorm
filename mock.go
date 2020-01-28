package dasorm

import (
	"context"
	"database/sql"
)

func connectMock(creds *Config) (*Connection, error) {

	return &Connection{
		DB:      mockDB{},
		Dialect: &mock{},
	}, nil
}

type mock struct{}

func (m *mock) Name() string                                         { return "mock" }
func (m *mock) TranslateSQL(sql string) string                       { return sql }
func (m *mock) Create(db *Connection, model *Model) error            { return ErrNotImplemented }
func (m *mock) CreateUpdate(*Connection, *Model) error               { return ErrNotImplemented }
func (m *mock) CreateMany(*Connection, *Model) error                 { return ErrNotImplemented }
func (m *mock) CreateManyTemp(*Connection, *Model) error             { return ErrNotImplemented }
func (m *mock) CreateManyUpdate(*Connection, *Model) error           { return ErrNotImplemented }
func (m *mock) Update(*Connection, *Model) error                     { return ErrNotImplemented }
func (m *mock) Destroy(*Connection, *Model) error                    { return ErrNotImplemented }
func (m *mock) DestroyMany(*Connection, *Model) error                { return ErrNotImplemented }
func (m *mock) SelectOne(*Connection, *Model, Query) error           { return ErrNotImplemented }
func (m *mock) SelectMany(*Connection, *Model, Query) error          { return ErrNotImplemented }
func (m *mock) SQLView(*Connection, *Model, map[string]string) error { return ErrNotImplemented }

type mockDB struct{}

func (m mockDB) Close() error { return nil }

func (m mockDB) Ping() error { return nil }

func (m mockDB) Query(string, ...interface{}) (*sql.Rows, error) { return nil, ErrNotImplemented }

func (m mockDB) QueryContext(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	return m.Query(sql, args...)
}

func (m mockDB) QueryRow(string, ...interface{}) *sql.Row { return nil }

func (m mockDB) QueryRowContext(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	return m.QueryRow(sql, args...)
}

func (m mockDB) Exec(string, ...interface{}) (sql.Result, error) { return nil, ErrNotImplemented }

func (m mockDB) ExecContext(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	return m.Exec(sql, args...)
}
func (m mockDB) Select(interface{}, string, ...interface{}) error { return ErrNotImplemented }

func (m mockDB) Get(interface{}, string, ...interface{}) error { return ErrNotImplemented }

func (m mockDB) NamedExec(string, interface{}) (sql.Result, error) { return nil, ErrNotImplemented }
