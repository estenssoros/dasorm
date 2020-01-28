package dasorm

import (
	"fmt"
	"reflect"
	"strings"

	interpol "github.com/imkira/go-interpol"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

func craftCreate(model *Model) string {
	model.setID(uuid.Must(uuid.NewV4()))
	model.touchCreatedAt()
	model.touchUpdatedAt()
	return InsertStmt(model.Value) + StringTuple(model.Value)
}

func genericExec(conn *Connection, stmt string) error {
	if err := conn.Exec(stmt); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func genericCreate(conn *Connection, model *Model) error {
	return genericExec(conn, craftCreate(model))
}

func craftCreateMany(model *Model) (string, error) {
	tuples, err := model.ToTuples()
	if err != nil {
		return "", errors.Wrap(err, "to tuples")
	}
	return InsertStmt(model.Value) + strings.Join(tuples, ","), nil
}

func genericCreateMany(conn *Connection, model *Model) error {
	query, err := craftCreateMany(model)
	if err != nil {
		return err
	}
	return genericExec(conn, query)
}

func craftUpdate(model *Model) string {
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", model.TableName(), model.UpdateString(), model.whereID())
}

func genericUpdate(conn *Connection, model *Model) error {
	stmt := craftUpdate(model)
	res, err := conn.DB.NamedExec(stmt, model.Value)
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

func genericDestroy(conn *Connection, model *Model) error {
	return genericExec(conn, craftDestroy(model))
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

func genericDestroyMany(conn *Connection, model *Model) error {
	query, err := craftDestroyMany(model)
	if err != nil {
		return errors.Wrap(err, "craft destroy many")
	}
	return genericExec(conn, query)
}

func genericSelectOne(conn *Connection, model *Model, query Query) error {
	sql, args := query.ToSQL(model)
	if err := conn.DB.Get(model.Value, sql, args...); err != nil {
		return err
	}
	return nil
}

func genericSelectMany(conn *Connection, models *Model, query Query) error {
	sql, args := query.ToSQL(models)
	if err := conn.DB.Select(models.Value, sql, args...); err != nil {
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

func genericSQLView(conn *Connection, model *Model, format map[string]string) error {
	sql, err := craftSQLView(model, format)
	if err != nil {
		return err
	}
	if model.isSlice() {
		if err := conn.DB.Select(model.Value, sql); err != nil {
			return err
		}
	} else {
		if err := conn.DB.Get(model.Value, sql); err != nil {
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

func genericCreateUpdate(conn *Connection, model *Model) error {
	return genericExec(conn, craftCreateUpdate(model))
}

func craftCreateManyUpdate(model *Model) (string, error) {
	tuples, err := model.ToTuples()
	if err != nil {
		return "", errors.Wrap(err, "to tuples")
	}
	return InsertStmt(model.Value) + strings.Join(tuples, ",") + model.DuplicateStmt(), nil
}

func genericCreateManyUpdate(conn *Connection, model *Model) error {
	query, err := craftCreateManyUpdate(model)
	if err != nil {
		return err
	}
	return genericExec(conn, query)
}

func craftCreateManyTemp(model *Model) (string, error) {
	tuples, err := model.ToTuples()
	if err != nil {
		return "", errors.Wrap(err, "to tuples")
	}
	return InsertTempStmt(model.Value) + strings.Join(tuples, ","), nil
}

func genericCreateManyTemp(conn *Connection, model *Model) error {
	query, err := craftCreateManyTemp(model)
	if err != nil {
		return err
	}
	return genericExec(conn, query)
}
