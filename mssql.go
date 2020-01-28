package dasorm

import (
	_ "github.com/denisenkom/go-mssqldb" //mssql
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

func connectMSSQL(config *Config) (*Connection, error) {
	db, err := sqlx.Connect("mssql", config.mssqlURL())
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(0)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Connection{
		DB:      db,
		Dialect: &mssql{},
	}, nil
}

type mssql struct{}

func (m *mssql) Name() string {
	return "mssql"
}

func (m *mssql) TranslateSQL(sql string) string {
	return sql
}

func (m *mssql) Create(conn *Connection, model *Model) error {
	return errors.Wrap(genericCreate(conn, model), "mysql create")
}

func (m *mssql) CreateMany(conn *Connection, model *Model) error {
	return errors.Wrap(genericCreateMany(conn, model), "mssql create")
}

func (m *mssql) Update(conn *Connection, model *Model) error {
	return errors.Wrap(genericUpdate(conn, model), "mssql update")
}

func (m *mssql) Destroy(conn *Connection, model *Model) error {
	return errors.Wrap(genericDestroy(conn, model), "mssql destroy")
}

func (m *mssql) DestroyMany(conn *Connection, model *Model) error {
	return errors.Wrap(genericDestroyMany(conn, model), "mssql destroy many")
}

func (m *mssql) SelectOne(conn *Connection, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(conn, model, query), "mssql select one")
}

func (m *mssql) SelectMany(conn *Connection, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(conn, models, query), "mssql select many")
}

func (m *mssql) SQLView(conn *Connection, models *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(conn, models, format), "mssql sql view")
}

func (m *mssql) CreateUpdate(*Connection, *Model) error {
	return ErrNotImplemented
}

func (m *mssql) CreateManyTemp(*Connection, *Model) error {
	return ErrNotImplemented
}

func (m *mssql) CreateManyUpdate(*Connection, *Model) error {
	return ErrNotImplemented
}
