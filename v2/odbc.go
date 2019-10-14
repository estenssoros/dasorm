package dasorm

import (
	"fmt"

	"github.com/pkg/errors"
)

func connectODBC(creds *Config) (*Connection, error) {
	connectionURL := fmt.Sprintf("DSN=%s;UID=%s;password=%s", creds.DSN, creds.User, creds.Password)
	db, err := connectURL("odbc", connectionURL)
	if err != nil {
		return nil, errors.Wrap(err, "connect odbc")
	}
	return &Connection{
		DB:      &DB{DB: db},
		Dialect: &odbc{},
	}, nil
}

type odbc struct{}

func (o *odbc) Name() string {
	return "odbc"
}

func (o *odbc) TranslateSQL(sql string) string {
	return sql
}

func (o *odbc) Create(db DBInterface, model *Model) error {
	return errors.Wrap(genericCreate(db, model), "odbc create")
}

func (o *odbc) CreateMany(db DBInterface, model *Model) error {
	return errors.Wrap(genericCreateMany(db, model), "odbc create")
}

func (o *odbc) Update(db DBInterface, model *Model) error {
	return errors.Wrap(genericUpdate(db, model), "odbc update")
}

func (o *odbc) Destroy(db DBInterface, model *Model) error {
	return errors.Wrap(genericDestroy(db, model), "odbc destroy")
}

func (o *odbc) DestroyMany(db DBInterface, model *Model) error {
	return errors.Wrap(genericDestroyMany(db, model), "odbc destroy many")
}

func (o *odbc) SelectOne(db DBInterface, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(db, model, query), "odbc select one")
}

func (o *odbc) SelectMany(db DBInterface, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(db, models, query), "odbc select many")
}

func (o *odbc) SQLView(db DBInterface, model *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(db, model, format), "odbc sql view")
}

func (o *odbc) CreateUpdate(db DBInterface, model *Model) error {
	return errors.Wrap(genericCreateUpdate(db, model), "odbc create update")
}

func (o *odbc) CreateManyTemp(DBInterface, *Model) error {
	return ErrNotImplemented
}

func (o *odbc) CreateManyUpdate(db DBInterface, model *Model) error {
	return errors.Wrap(genericCreateManyUpdate(db, model), "odbc create update many")
}

func (o *odbc) Truncate(db DBInterface, model *Model) error {
	return errors.Wrap(genericTruncate(db, model), "odbc truncate")
}
