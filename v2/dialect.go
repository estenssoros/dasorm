package dasorm

import (
	"fmt"
	"reflect"
	"strings"

	interpol "github.com/imkira/go-interpol"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type dialect interface {
	Name() string
	TranslateSQL(string) string
	Create(DBInterface, *Model) error
	CreateUpdate(DBInterface, *Model) error
	CreateMany(DBInterface, *Model) error
	CreateManyTemp(DBInterface, *Model) error
	CreateManyUpdate(DBInterface, *Model) error
	Update(DBInterface, *Model) error
	Destroy(DBInterface, *Model) error
	DestroyMany(DBInterface, *Model) error
	SelectOne(DBInterface, *Model, Query) error
	SelectMany(DBInterface, *Model, Query) error
	SQLView(DBInterface, *Model, map[string]string) error
	Truncate(DBInterface, *Model) error
}

func craftCreate(model *Model) string {
	model.setID(uuid.Must(uuid.NewV4()))
	model.touchCreatedAt()
	model.touchUpdatedAt()
	return InsertStmt(model.Value) + StringTuple(model.Value)
}

func genericExec(db DBInterface, stmt string) error {
	if db.Debug() {
		fmt.Println(stmt)
	}
	if _, err := db.Exec(stmt); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func genericCreate(db DBInterface, model *Model) error {
	return genericExec(db, craftCreate(model))
}

func craftCreateMany(model *Model) (string, error) {
	tuples, err := model.ToTuples()
	if err != nil {
		return "", errors.Wrap(err, "to tuples")
	}
	return InsertStmt(model.Value) + strings.Join(tuples, ","), nil
}

func genericCreateMany(db DBInterface, model *Model) error {
	query, err := craftCreateMany(model)
	if err != nil {
		return err
	}
	return genericExec(db, query)
}

func craftUpdate(model *Model) string {
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", model.TableName(), model.UpdateString(), model.whereID())
}

func genericUpdate(db DBInterface, model *Model) error {
	stmt := craftUpdate(model)
	if db.Debug() {
		fmt.Println(stmt)
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

func genericDestroy(db DBInterface, model *Model) error {
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

func genericDestroyMany(db DBInterface, model *Model) error {
	query, err := craftDestroyMany(model)
	if err != nil {
		return errors.Wrap(err, "craft destroy many")
	}
	return genericExec(db, query)
}

func genericSelectOne(db DBInterface, model *Model, query Query) error {
	sql, args := query.ToSQL(model)
	if db.Debug() {
		fmt.Println(sql)
	}
	if err := db.Get(model.Value, sql, args...); err != nil {
		return err
	}
	return nil
}

func genericSelectMany(db DBInterface, models *Model, query Query) error {
	sql, args := query.ToSQL(models)
	if db.Debug() {
		fmt.Println(sql)
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

func genericSQLView(db DBInterface, model *Model, format map[string]string) error {
	sql, err := craftSQLView(model, format)
	if err != nil {
		return err
	}
	if db.Debug() {
		fmt.Println(sql)
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

func genericCreateUpdate(db DBInterface, model *Model) error {
	return genericExec(db, craftCreateUpdate(model))
}

func craftCreateManyUpdate(model *Model) (string, error) {
	tuples, err := model.ToTuples()
	if err != nil {
		return "", errors.Wrap(err, "to tuples")
	}
	return InsertStmt(model.Value) + strings.Join(tuples, ",") + model.DuplicateStmt(), nil
}

func genericCreateManyUpdate(db DBInterface, model *Model) error {
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

func genericCreateManyTemp(db DBInterface, model *Model) error {
	query, err := craftCreateManyTemp(model)
	if db.Debug() {
		fmt.Println(query)
	}
	if err != nil {
		return errors.Wrap(err, "craft create many temp")
	}
	return genericExec(db, query)
}

func genericTruncate(db DBInterface, model *Model) error {
	return genericExec(db, TruncateStmt(model.Value))
}
