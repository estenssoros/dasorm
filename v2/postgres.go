package dasorm

import (
	"fmt"

	"github.com/pkg/errors"
)

func connectPostgres(creds *Config) (*Connection, error) {
	connectionURL := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		creds.Host, 5432, creds.User, creds.Password, creds.Database)
	db, err := connectURL("postgres", connectionURL)
	if err != nil {
		return nil, errors.Wrap(err, "connect postgres")
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

func (p *postgres) Create(db DBInterface, model *Model) error {
	return errors.Wrap(genericCreate(db, model), "postgres create")
}

func (p *postgres) CreateMany(db DBInterface, model *Model) error {
	return errors.Wrap(genericCreateMany(db, model), "postgres create")
}

func (p *postgres) Update(db DBInterface, model *Model) error {
	return errors.Wrap(genericUpdate(db, model), "postgres update")
}

func (p *postgres) Destroy(db DBInterface, model *Model) error {
	return errors.Wrap(genericDestroy(db, model), "postgres destroy")
}

func (p *postgres) DestroyMany(db DBInterface, model *Model) error {
	return errors.Wrap(genericDestroyMany(db, model), "postgres destroy many")
}

func (p *postgres) SelectOne(db DBInterface, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(db, model, query), "postgres select one")
}

func (p *postgres) SelectMany(db DBInterface, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(db, models, query), "postgres select many")
}

func (p *postgres) SQLView(db DBInterface, models *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(db, models, format), "postgres sql view")
}

func (p *postgres) CreateUpdate(DBInterface, *Model) error {
	return ErrNotImplemented
}

func (p *postgres) CreateManyTemp(DBInterface, *Model) error {
	return ErrNotImplemented
}

func (p *postgres) CreateManyUpdate(DBInterface, *Model) error {
	return ErrNotImplemented
}

func (p *postgres) Truncate(db DBInterface, model *Model) error {
	return errors.Wrap(genericTruncate(db, model), "postgres truncate")
}
