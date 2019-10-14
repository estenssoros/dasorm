package dasorm

import (
	"fmt"

	"github.com/pkg/errors"
)

func connectSnowflake(creds *Config) (*Connection, error) {
	connectionURL := fmt.Sprintf("%s:%s@%s/%s", creds.User, creds.Password, creds.Host, creds.Database)
	db, err := connectURL("snowflake", connectionURL)
	if err != nil {
		return nil, errors.Wrap(err, "connect snowflake")
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

func (s *snowflake) Create(db DBInterface, model *Model) error {
	return errors.Wrap(genericCreate(db, model), "snowflake create")
}

func (s *snowflake) CreateMany(db DBInterface, model *Model) error {
	return errors.Wrap(genericCreateMany(db, model), "snowflake create")
}

func (s *snowflake) Update(db DBInterface, model *Model) error {
	return errors.Wrap(genericUpdate(db, model), "snowflake update")
}

func (s *snowflake) Destroy(db DBInterface, model *Model) error {
	return errors.Wrap(genericDestroy(db, model), "snowflake destroy")
}

func (s *snowflake) DestroyMany(db DBInterface, model *Model) error {
	return errors.Wrap(genericDestroyMany(db, model), "snowflake destroy many")
}

func (s *snowflake) SelectOne(db DBInterface, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(db, model, query), "snowflake select one")
}

func (s *snowflake) SelectMany(db DBInterface, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(db, models, query), "snowflake select many")
}

func (s *snowflake) SQLView(db DBInterface, model *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(db, model, format), "snowflake sql view")
}

func (s *snowflake) CreateUpdate(db DBInterface, model *Model) error {
	return errors.Wrap(genericCreateUpdate(db, model), "snowflake create update")
}
func (s *snowflake) CreateManyTemp(db DBInterface, model *Model) error {
	return errors.Wrap(genericCreateManyTemp(db, model), "snowflake create many temp")
}

func (s *snowflake) CreateManyUpdate(db DBInterface, model *Model) error {
	return errors.Wrap(genericCreateManyUpdate(db, model), "snowflake create update many")
}

func (s *snowflake) Truncate(db DBInterface, model *Model) error {
	return errors.Wrap(genericTruncate(db, model), "snowflake truncate")
}
