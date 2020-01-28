package dasorm

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	_ "github.com/snowflakedb/gosnowflake" // snowflake
)

func connectSnowflake(creds *Config) (*Connection, error) {
	connectionURL := fmt.Sprintf("%s:%s@%s/%s", creds.User, creds.Password, creds.Host, creds.Database)
	db, err := sqlx.Connect("snowflake", connectionURL)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Connection{
		DB:      db,
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

func (s *snowflake) Create(conn *Connection, model *Model) error {
	return errors.Wrap(genericCreate(conn, model), "snowflake create")
}

func (s *snowflake) CreateMany(conn *Connection, model *Model) error {
	return errors.Wrap(genericCreateMany(conn, model), "snowflake create")
}

func (s *snowflake) Update(conn *Connection, model *Model) error {
	return errors.Wrap(genericUpdate(conn, model), "snowflake update")
}

func (s *snowflake) Destroy(conn *Connection, model *Model) error {
	return errors.Wrap(genericDestroy(conn, model), "snowflake destroy")
}

func (s *snowflake) DestroyMany(conn *Connection, model *Model) error {
	return errors.Wrap(genericDestroyMany(conn, model), "snowflake destroy many")
}

func (s *snowflake) SelectOne(conn *Connection, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(conn, model, query), "snowflake select one")
}

func (s *snowflake) SelectMany(conn *Connection, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(conn, models, query), "snowflake select many")
}

func (s *snowflake) SQLView(conn *Connection, model *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(conn, model, format), "snowflake sql view")
}

func (s *snowflake) CreateUpdate(conn *Connection, model *Model) error {
	return errors.Wrap(genericCreateUpdate(conn, model), "snowflake create update")
}
func (s *snowflake) CreateManyTemp(conn *Connection, model *Model) error {
	return errors.Wrap(genericCreateManyTemp(conn, model), "snowflake create many temp")
}

func (s *snowflake) CreateManyUpdate(conn *Connection, model *Model) error {
	return errors.Wrap(genericCreateManyUpdate(conn, model), "snowflake create update many")
}
