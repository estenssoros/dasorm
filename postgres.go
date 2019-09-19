package dasorm

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" //postgres driver
	"github.com/pkg/errors"
)

func connectPostgres(creds *Config) (*Connection, error) {
	connectionURL := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		creds.Host, 5432, creds.User, creds.Password, creds.Database)
	db, err := sqlx.Connect("postgres", connectionURL)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(0)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Connection{
		DB:      &DB{DB: db},
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

func (p *postgres) Create(db *DB, model *Model) error {
	return errors.Wrap(genericCreate(db, model), "postgres create")
}

func (p *postgres) CreateMany(db *DB, model *Model) error {
	return errors.Wrap(genericCreateMany(db, model), "postgres create")
}

func (p *postgres) Update(db *DB, model *Model) error {
	return errors.Wrap(genericUpdate(db, model), "postgres update")
}

func (p *postgres) Destroy(db *DB, model *Model) error {
	return errors.Wrap(genericDestroy(db, model), "postgres destroy")
}

func (p *postgres) DestroyMany(db *DB, model *Model) error {
	return errors.Wrap(genericDestroyMany(db, model), "postgres destroy many")
}

func (p *postgres) SelectOne(db *DB, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(db, model, query), "postgres select one")
}

func (p *postgres) SelectMany(db *DB, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(db, models, query), "postgres select many")
}

func (p *postgres) SQLView(db *DB, models *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(db, models, format), "postgres sql view")
}

func (p *postgres) CreateUpdate(*DB, *Model) error {
	return ErrNotImplemented
}

func (p *postgres) CreateManyTemp(*DB, *Model) error {
	return ErrNotImplemented
}

func (p *postgres) CreateManyUpdate(*DB, *Model) error {
	return ErrNotImplemented
}
