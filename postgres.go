package dasorm

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" //postgres driver
	"github.com/pkg/errors"
)

func connectPostgres(config *Config) (*Connection, error) {
	db, err := sqlx.Connect("postgres", config.postgresURL())
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(0)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Connection{
		DB:      db,
		Dialect: &postgres{},
	}, nil
}

type postgres struct{}

func (p *postgres) Name() string {
	return "postgres"
}

func (p *postgres) TranslateSQL(sql string) string {
	return sql
}

func (p *postgres) Create(conn *Connection, model *Model) error {
	return errors.Wrap(genericCreate(conn, model), "postgres create")
}

func (p *postgres) CreateMany(conn *Connection, model *Model) error {
	return errors.Wrap(genericCreateMany(conn, model), "postgres create")
}

func (p *postgres) Update(conn *Connection, model *Model) error {
	return errors.Wrap(genericUpdate(conn, model), "postgres update")
}

func (p *postgres) Destroy(conn *Connection, model *Model) error {
	return errors.Wrap(genericDestroy(conn, model), "postgres destroy")
}

func (p *postgres) DestroyMany(conn *Connection, model *Model) error {
	return errors.Wrap(genericDestroyMany(conn, model), "postgres destroy many")
}

func (p *postgres) SelectOne(conn *Connection, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(conn, model, query), "postgres select one")
}

func (p *postgres) SelectMany(conn *Connection, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(conn, models, query), "postgres select many")
}

func (p *postgres) SQLView(conn *Connection, models *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(conn, models, format), "postgres sql view")
}

func (p *postgres) CreateUpdate(*Connection, *Model) error {
	return ErrNotImplemented
}

func (p *postgres) CreateManyTemp(*Connection, *Model) error {
	return ErrNotImplemented
}

func (p *postgres) CreateManyUpdate(*Connection, *Model) error {
	return ErrNotImplemented
}
