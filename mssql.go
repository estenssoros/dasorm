package dasorm

import (
	"fmt"

	_ "github.com/denisenkom/go-mssqldb" //mssql
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

func connectMSSQL(creds *Config) (*Connection, error) {
	connectionURL := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", creds.User, creds.Password, creds.Host, creds.Database)
	db, err := sqlx.Connect("mssql", connectionURL)
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

func (m *mssql) Create(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericCreate(db, model), "mysql create")
}

func (m *mssql) CreateMany(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericCreateMany(db, model), "mssql create")
}

func (m *mssql) Update(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericUpdate(db, model), "mssql update")
}

func (m *mssql) Destroy(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericDestroy(db, model), "mssql destroy")
}

func (m *mssql) DestroyMany(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericDestroyMany(db, model), "mssql destroy many")
}

func (m *mssql) SelectOne(db *sqlx.DB, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(db, model, query), "mssql select one")
}

func (m *mssql) SelectMany(db *sqlx.DB, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(db, models, query), "mssql select many")
}

func (m *mssql) SQLView(db *sqlx.DB, models *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(db, models, format), "mssql sql view")
}

func (m *mssql) CreateUpdate(*sqlx.DB, *Model) error {
	return ErrNotImplemented
}

func (m *mssql) CreateManyUpdate(*sqlx.DB, *Model) error {
	return ErrNotImplemented
}
