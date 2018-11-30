package dasorm

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
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

func (p *postgres) Create(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericCreate(db, model), "postgres create")
}

func (p *postgres) CreateMany(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericCreateMany(db, model), "postgres create")
}

func (p *postgres) Update(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericUpdate(db, model), "postgres update")
}

func (p *postgres) Destroy(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericDestroy(db, model), "postgres destroy")
}

func (p *postgres) DestroyMany(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericDestroyMany(db, model), "postgres destroy many")
}

func (p *postgres) SelectOne(db *sqlx.DB, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(db, model, query), "postgres select one")
}

func (p *postgres) SelectMany(db *sqlx.DB, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(db, models, query), "postgres select many")
}
