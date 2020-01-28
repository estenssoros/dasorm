package dasorm

import (
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

func connectMySQL(config *Config) (*Connection, error) {
	db, err := sqlx.Connect("mysql", config.mysqlURL())
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(0)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Connection{
		DB:      db,
		Dialect: &mysql{},
	}, nil
}

type mysql struct{}

func (m *mysql) Name() string {
	return "mysql"
}

func (m *mysql) TranslateSQL(sql string) string {
	return sql
}

func (m *mysql) Create(conn *Connection, model *Model) error {
	return errors.Wrap(genericCreate(conn, model), "mysql create")
}

func (m *mysql) CreateMany(conn *Connection, model *Model) error {
	return errors.Wrap(genericCreateMany(conn, model), "mysql create")
}

func (m *mysql) Update(conn *Connection, model *Model) error {
	return errors.Wrap(genericUpdate(conn, model), "mysql update")
}

func (m *mysql) Destroy(conn *Connection, model *Model) error {
	return errors.Wrap(genericDestroy(conn, model), "mysql destroy")
}

func (m *mysql) DestroyMany(conn *Connection, model *Model) error {
	return errors.Wrap(genericDestroyMany(conn, model), "mysql destroy many")
}

func (m *mysql) SelectOne(conn *Connection, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(conn, model, query), "mysql select one")
}

func (m *mysql) SelectMany(conn *Connection, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(conn, models, query), "mysql select many")
}

func (m *mysql) SQLView(conn *Connection, model *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(conn, model, format), "mysql sql view")
}

func (m *mysql) CreateUpdate(conn *Connection, model *Model) error {
	return errors.Wrap(genericCreateUpdate(conn, model), "mysql create update")
}

func (m *mysql) CreateManyTemp(*Connection, *Model) error {
	return ErrNotImplemented
}

func (m *mysql) CreateManyUpdate(conn *Connection, model *Model) error {
	return errors.Wrap(genericCreateManyUpdate(conn, model), "mysql create update many")
}
