package dasorm

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/fatih/color"
	interpol "github.com/imkira/go-interpol"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

func printSQL(s string) {
	breaks := strings.Split(s, "\n")
	for i, b := range breaks {
		breaks[i] = strings.TrimSpace(b)
	}
	color.Green(strings.Join(breaks, " "))
}

type dialect interface {
	Name() string
	TranslateSQL(string) string
	Create(*DB, *Model) error
	CreateUpdate(*DB, *Model) error
	CreateMany(*DB, *Model) error
	CreateManyTemp(*DB, *Model) error
	CreateManyUpdate(*DB, *Model) error
	Update(*DB, *Model) error
	Destroy(*DB, *Model) error
	DestroyMany(*DB, *Model) error
	SelectOne(*DB, *Model, Query) error
	SelectMany(*DB, *Model, Query) error
	SQLView(*DB, *Model, map[string]string) error
}

func craftCreate(model *Model) string {
	model.setID(uuid.Must(uuid.NewV4()))
	model.touchCreatedAt()
	model.touchUpdatedAt()
	return InsertStmt(model.Value) + StringTuple(model.Value)
}

func genericExec(db *DB, stmt string) error {
	if db.Debug {
		printSQL(stmt)
	}
	if _, err := db.Exec(stmt); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func genericExecWithID(db *DB, stmt string) (int64, error) {
	if db.Debug {
		printSQL(stmt)
	}
	res, err := db.Exec(stmt)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if id, err := res.LastInsertId(); err == nil {
		return id, nil
	}
	return 0, nil
}

func genericCreate(db *DB, model *Model) error {
	id, err := genericExecWithID(db, craftCreate(model))
	if id != 0 {
		model.setID(id)
	}
	return err
}

func craftCreateMany(model *Model) (string, error) {
	tuples, err := model.ToTuples()
	if err != nil {
		return "", errors.Wrap(err, "to tuples")
	}
	return InsertStmt(model.Value) + strings.Join(tuples, ","), nil
}

func genericCreateMany(db *DB, model *Model) error {
	query, err := craftCreateMany(model)
	if err != nil {
		return err
	}
	return genericExec(db, query)
}

func craftUpdate(model *Model) string {
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", model.TableName(), model.UpdateString(), model.whereID())
}

func genericUpdate(db *DB, model *Model) error {
	stmt := craftUpdate(model)
	if db.Debug {
		printSQL(stmt)
	}
	res, err := db.NamedExec(stmt, model.Value)
	if err != nil {
		return errors.Wrap(err, "updating record")
	}
	if numRows, _ := res.RowsAffected(); numRows == 0 {
		return errors.Errorf("query updated 0 rows: %s", model.whereID())
	}
	return nil
}

func craftDestroy(model *Model) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s", model.TableName(), model.whereID())
}

func genericDestroy(db *DB, model *Model) error {
	return genericExec(db, craftDestroy(model))
}

func craftDestroyMany(model *Model) (string, error) {
	ids := []string{}
	if !model.isSlice() {
		return "", errors.New("must supply slice")
	}
	v := reflect.Indirect(reflect.ValueOf(model.Value))
	for i := 0; i < v.Len(); i++ {
		val := v.Index(i)
		var newModel *Model
		if val.Kind() == reflect.Ptr {
			newModel = &Model{Value: val.Interface()}
		} else {
			newModel = &Model{Value: val.Addr().Interface()}
		}
		fbn, err := newModel.fieldByName("ID")
		if err != nil {
			return "", err
		}
		id, ok := fbn.Interface().(uuid.UUID)
		if !ok {
			return "", errors.New("error converting value to uuid")
		}
		ids = append(ids, fmt.Sprintf("'%s'", id))
	}
	return fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", model.TableName(), strings.Join(ids, ",")), nil
}

func genericDestroyMany(db *DB, model *Model) error {
	query, err := craftDestroyMany(model)
	if err != nil {
		return errors.Wrap(err, "craft destroy many")
	}
	return genericExec(db, query)
}

func genericSelectOne(db *DB, model *Model, query Query) error {
	sql, args := query.ToSQL(model)
	if db.Debug {
		printSQL(sql)
	}
	if err := db.Get(model.Value, sql, args...); err != nil {
		return err
	}
	return nil
}

func genericSelectMany(db *DB, models *Model, query Query) error {
	sql, args := query.ToSQL(models)
	if db.Debug {
		printSQL(sql)
	}
	if err := db.Select(models.Value, sql, args...); err != nil {
		return err
	}
	return nil
}

func craftSQLView(model *Model, format map[string]string) (string, error) {
	var (
		err error
		sql string
	)

	sql, err = model.SQLView()
	if err != nil {
		return "", err
	}
	if format != nil {
		sql, err = interpol.WithMap(sql, format)
		if err != nil {
			return "", errors.Wrap(err, "formatting sql")
		}
	}
	return sql, nil
}

func genericSQLView(db *DB, model *Model, format map[string]string) error {
	sql, err := craftSQLView(model, format)
	if err != nil {
		return err
	}
	if db.Debug {
		printSQL(sql)
	}
	if model.isSlice() {
		if err := db.Select(model.Value, sql); err != nil {
			return err
		}
	} else {
		if err := db.Get(model.Value, sql); err != nil {
			return err
		}
	}
	return nil
}

func craftCreateUpdate(model *Model) string {
	model.setID(uuid.Must(uuid.NewV4()))
	model.touchCreatedAt()
	model.touchUpdatedAt()
	return InsertStmt(model.Value) + StringTuple(model.Value) + model.DuplicateStmt()
}

func genericCreateUpdate(db *DB, model *Model) error {
	return genericExec(db, craftCreateUpdate(model))
}

func craftCreateManyUpdate(model *Model) (string, error) {
	tuples, err := model.ToTuples()
	if err != nil {
		return "", errors.Wrap(err, "to tuples")
	}
	return InsertStmt(model.Value) + strings.Join(tuples, ",") + model.DuplicateStmt(), nil
}

func genericCreateManyUpdate(db *DB, model *Model) error {
	query, err := craftCreateManyUpdate(model)
	if err != nil {
		return err
	}
	return genericExec(db, query)
}

func craftCreateManyTemp(model *Model) (string, error) {
	tuples, err := model.ToTuples()
	if err != nil {
		return "", errors.Wrap(err, "to tuples")
	}
	return InsertTempStmt(model.Value) + strings.Join(tuples, ","), nil
}

func genericCreateManyTemp(db *DB, model *Model) error {
	query, err := craftCreateManyTemp(model)
	if err != nil {
		return err
	}
	return genericExec(db, query)
}
