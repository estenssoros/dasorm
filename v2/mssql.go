package dasorm

import (
	"fmt"

	"github.com/pkg/errors"
)

func connectMSSQL(creds *Config) (*Connection, error) {
	connectionURL := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", creds.User, creds.Password, creds.Host, creds.Database)
	db, err := connectURL("mssql", connectionURL)
	if err != nil {
		return nil, errors.Wrap(err, "connect sqlserver")
	}
	return &Connection{
		DB:      &DB{DB: db},
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

func (m *mssql) Create(db DBInterface, model *Model) error {
	return errors.Wrap(genericCreate(db, model), "mysql create")
}

func (m *mssql) CreateMany(db DBInterface, model *Model) error {
	return errors.Wrap(genericCreateMany(db, model), "mssql create")
}

func (m *mssql) Update(db DBInterface, model *Model) error {
	return errors.Wrap(genericUpdate(db, model), "mssql update")
}

func (m *mssql) Destroy(db DBInterface, model *Model) error {
	return errors.Wrap(genericDestroy(db, model), "mssql destroy")
}

func (m *mssql) DestroyMany(db DBInterface, model *Model) error {
	return errors.Wrap(genericDestroyMany(db, model), "mssql destroy many")
}

func (m *mssql) SelectOne(db DBInterface, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(db, model, query), "mssql select one")
}

func (m *mssql) SelectMany(db DBInterface, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(db, models, query), "mssql select many")
}

func (m *mssql) SQLView(db DBInterface, models *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(db, models, format), "mssql sql view")
}

func (m *mssql) CreateUpdate(DBInterface, *Model) error {
	return ErrNotImplemented
}

func (m *mssql) CreateManyTemp(DBInterface, *Model) error {
	return ErrNotImplemented
}

func (m *mssql) CreateManyUpdate(DBInterface, *Model) error {
	return ErrNotImplemented
}

func (m *mssql) Truncate(db DBInterface, model *Model) error {
	return errors.Wrap(genericTruncate(db, model), "mssql truncate")
}
