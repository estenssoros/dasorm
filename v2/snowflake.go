package dasorm

import (
	"fmt"

	"github.com/pkg/errors"
)

func connectSnowflake(creds *Config) (*Connection, error) {
	connectionURL := fmt.Sprintf("%s:%s@%s/%s", creds.User, creds.Password, creds.Host, creds.Database)
	db, err := connectURL("snowflake", connectionURL)
	if err != nil {
		return nil, errors.Wrap(err, "connect url")
	}
	return &Connection{
		DB:      &DB{DB: db},
		Dialect: &snowflake{},
	}, nil
}

type snowflake struct{}

func (s *snowflake) Name() string {
	return "snowflake"
}

func (s *snowflake) TranslateSQL(sql string) string {
	return sql
}

func (s *snowflake) Create(db *DB, model *Model) error {
	return errors.Wrap(genericCreate(db, model), "snowflake create")
}

func (s *snowflake) CreateMany(db *DB, model *Model) error {
	return errors.Wrap(genericCreateMany(db, model), "snowflake create")
}

func (s *snowflake) Update(db *DB, model *Model) error {
	return errors.Wrap(genericUpdate(db, model), "snowflake update")
}

func (s *snowflake) Destroy(db *DB, model *Model) error {
	return errors.Wrap(genericDestroy(db, model), "snowflake destroy")
}

func (s *snowflake) DestroyMany(db *DB, model *Model) error {
	return errors.Wrap(genericDestroyMany(db, model), "snowflake destroy many")
}

func (s *snowflake) SelectOne(db *DB, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(db, model, query), "snowflake select one")
}

func (s *snowflake) SelectMany(db *DB, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(db, models, query), "snowflake select many")
}

func (s *snowflake) SQLView(db *DB, model *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(db, model, format), "snowflake sql view")
}

func (s *snowflake) CreateUpdate(db *DB, model *Model) error {
	return errors.Wrap(genericCreateUpdate(db, model), "snowflake create update")
}
func (s *snowflake) CreateManyTemp(db *DB, model *Model) error {
	return errors.Wrap(genericCreateManyTemp(db, model), "snowflake create many temp")
}

func (s *snowflake) CreateManyUpdate(db *DB, model *Model) error {
	return errors.Wrap(genericCreateManyUpdate(db, model), "snowflake create update many")
}
