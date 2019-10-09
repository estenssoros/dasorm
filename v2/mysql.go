package dasorm

import (
	"fmt"

	"github.com/pkg/errors"
)

func connectMySQL(creds *Config) (*Connection, error) {
	connectionURL := fmt.Sprintf("%s:%s@(%s)/%s?parseTime=true", creds.User, creds.Password, creds.Host, creds.Database)
	db, err := connectURL("mysql", connectionURL)
	if err != nil {
		return nil, errors.Wrap(err, "connect url")
	}
	return &Connection{
		DB:      &DB{DB: db},
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

func (m *mysql) Create(db *DB, model *Model) error {
	return errors.Wrap(genericCreate(db, model), "mysql create")
}

func (m *mysql) CreateMany(db *DB, model *Model) error {
	return errors.Wrap(genericCreateMany(db, model), "mysql create")
}

func (m *mysql) Update(db *DB, model *Model) error {
	return errors.Wrap(genericUpdate(db, model), "mysql update")
}

func (m *mysql) Destroy(db *DB, model *Model) error {
	return errors.Wrap(genericDestroy(db, model), "mysql destroy")
}

func (m *mysql) DestroyMany(db *DB, model *Model) error {
	return errors.Wrap(genericDestroyMany(db, model), "mysql destroy many")
}

func (m *mysql) SelectOne(db *DB, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(db, model, query), "mysql select one")
}

func (m *mysql) SelectMany(db *DB, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(db, models, query), "mysql select many")
}

func (m *mysql) SQLView(db *DB, model *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(db, model, format), "mysql sql view")
}

func (m *mysql) CreateUpdate(db *DB, model *Model) error {
	return errors.Wrap(genericCreateUpdate(db, model), "mysql create update")
}

func (m *mysql) CreateManyTemp(*DB, *Model) error {
	return ErrNotImplemented
}

func (m *mysql) CreateManyUpdate(db *DB, model *Model) error {
	return errors.Wrap(genericCreateManyUpdate(db, model), "mysql create update many")
}
