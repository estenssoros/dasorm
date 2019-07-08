package dasorm

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

func connectMySQL(creds *Config) (*Connection, error) {
	connectionURL := fmt.Sprintf("%s:%s@(%s)/%s?parseTime=true", creds.User, creds.Password, creds.Host, creds.Database)
	db, err := sqlx.Connect("mysql", connectionURL)
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

func (m *mysql) Create(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericCreate(db, model), "mysql create")
}

func (m *mysql) CreateMany(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericCreateMany(db, model), "mysql create")
}

func (m *mysql) Update(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericUpdate(db, model), "mysql update")
}

func (m *mysql) Destroy(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericDestroy(db, model), "mysql destroy")
}

func (m *mysql) DestroyMany(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericDestroyMany(db, model), "mysql destroy many")
}

func (m *mysql) SelectOne(db *sqlx.DB, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(db, model, query), "mysql select one")
}

func (m *mysql) SelectMany(db *sqlx.DB, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(db, models, query), "mysql select many")
}

func (m *mysql) SQLView(db *sqlx.DB, model *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(db, model, format), "mysql sql view")
}

func (m *mysql) CreateUpdate(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericCreateUpdate(db, model), "mysql create update")
}

func (m *mysql) CreateManyUpdate(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericCreateManyUpdate(db, model), "mysql create update many")
}
