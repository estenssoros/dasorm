package dasorm

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

type test struct {
	ID        uuid.UUID `db:"id"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

func newTest() *test {
	return &test{
		ID:        defaultUUID,
		CreatedAt: defaultTime,
		UpdatedAt: defaultTime,
	}
}

func newTestSlice() []*test {
	return []*test{
		&test{
			ID:        defaultUUID,
			CreatedAt: defaultTime,
			UpdatedAt: defaultTime,
		},
		&test{
			ID:        defaultUUID,
			CreatedAt: defaultTime,
			UpdatedAt: defaultTime,
		},
	}
}

var errOnPurpose = errors.New("this is an on purpose error")

type badDB struct{}

func (d badDB) Get(interface{}, string, ...interface{}) error     { return errOnPurpose }
func (d badDB) Select(interface{}, string, ...interface{}) error  { return errOnPurpose }
func (d badDB) NamedExec(string, interface{}) (sql.Result, error) { return nil, errOnPurpose }
func (d badDB) Debug() bool                                       { return false }
func (d badDB) Close() error                                      { return errOnPurpose }
func (d badDB) Ping() error                                       { return errOnPurpose }
func (d badDB) Query(string, ...interface{}) (*sql.Rows, error)   { return nil, errOnPurpose }
func (d badDB) QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error) {
	return nil, errOnPurpose
}
func (d badDB) SetDebug(bool)                                                    {}
func (d badDB) QueryRow(string, ...interface{}) *sql.Row                         { return nil }
func (d badDB) QueryRowContext(context.Context, string, ...interface{}) *sql.Row { return nil }
func (d badDB) Exec(string, ...interface{}) (sql.Result, error)                  { return nil, errOnPurpose }
func (d badDB) ExecContext(context.Context, string, ...interface{}) (sql.Result, error) {
	return nil, errOnPurpose
}

func (t test) SQLView() string {
	return `howdy {name}`
}

var (
	defaultTime = time.Now()
	defaultUUID = uuid.Must(uuid.NewV4())
)

func TestCraftCreate(t *testing.T) {
	model := newTest()
	have := craftCreate(&Model{model})
	want := "INSERT INTO test (id,created_at,updated_at) VALUES('%s','%s','%s')"
	want = fmt.Sprintf(want, model.ID, model.CreatedAt.Format(timeFmt), model.UpdatedAt.Format(timeFmt))
	assert.Equal(t, want, have)
}

func TestCraftCreateMany(t *testing.T) {
	{
		models := newTest()
		_, err := craftCreateMany(&Model{models})
		assert.Error(t, err)

	}
	{
		models := newTestSlice()

		{
			have, err := craftCreateMany(&Model{models})
			if err != nil {
				t.Error((err))
			}
			want := `INSERT INTO test (id,created_at,updated_at) VALUES('%s','%s','%s'),('%s','%s','%s')`
			want = fmt.Sprintf(
				want,
				models[0].ID,
				models[0].CreatedAt.Format(timeFmt),
				models[0].UpdatedAt.Format(timeFmt),
				models[1].ID,
				models[1].CreatedAt.Format(timeFmt),
				models[1].UpdatedAt.Format(timeFmt),
			)

			assert.Equal(t, want, have)
		}
		{
			have, err := craftCreateMany(&Model{&models})
			if err != nil {
				t.Error((err))
			}
			want := `INSERT INTO test (id,created_at,updated_at) VALUES('%s','%s','%s'),('%s','%s','%s')`
			want = fmt.Sprintf(
				want,
				models[0].ID,
				models[0].CreatedAt.Format(timeFmt),
				models[0].UpdatedAt.Format(timeFmt),
				models[1].ID,
				models[1].CreatedAt.Format(timeFmt),
				models[1].UpdatedAt.Format(timeFmt),
			)

			assert.Equal(t, want, have)
		}
	}
}

func TestCraftUpdate(t *testing.T) {
	model := newTest()
	have := craftUpdate(&Model{model})
	want := "UPDATE test SET updated_at = :updated_at WHERE id='%s'"
	want = fmt.Sprintf(want, model.ID)
	assert.Equal(t, want, have)
}

func TestCraftDestroy(t *testing.T) {
	model := newTest()
	have := craftDestroy(&Model{model})
	want := "DELETE FROM test WHERE id='%s'"
	want = fmt.Sprintf(want, model.ID)
	assert.Equal(t, want, have)
}

func TestCraftDestroyMany(t *testing.T) {
	{
		model := newTest()
		_, err := craftDestroyMany(&Model{model})
		if err == nil {
			t.Error("should error")
		}
	}
	{
		models := newTestSlice()
		have, err := craftDestroyMany(&Model{models})
		if err != nil {
			t.Error(err)
		}
		want := `DELETE FROM test WHERE id IN ('%s','%s')`
		want = fmt.Sprintf(want, defaultUUID, defaultUUID)
		assert.Equal(t, want, have)
	}
	{
		models := newTestSlice()
		have, err := craftDestroyMany(&Model{models})
		if err != nil {
			t.Error(err)
		}
		want := `DELETE FROM test WHERE id IN ('%s','%s')`
		want = fmt.Sprintf(want, defaultUUID, defaultUUID)
		assert.Equal(t, want, have)
	}
	noID := []struct{ Name string }{
		{Name: "asdf"},
	}
	if _, err := craftDestroyMany((&Model{noID})); err == nil {
		t.Error("shoudl error")
	}

	badID := []struct{ ID int }{
		{ID: 0},
	}
	if _, err := craftDestroyMany((&Model{badID})); err == nil {
		t.Error("shoudl error")
	}
}

func TestCraftSQLView(t *testing.T) {
	model := &test{}
	format := map[string]string{"name": "partner"}
	have, err := craftSQLView(&Model{model}, format)
	if err != nil {
		t.Error(err)
	}
	want := "howdy partner"
	assert.Equal(t, want, have)
}

func TestCraftCreateUpdate(t *testing.T) {
	model := newTest()
	have := craftCreateUpdate(&Model{model})
	want := "INSERT INTO test (id,created_at,updated_at) VALUES('%s','%s','%s')ON DUPLICATE KEY UPDATE id=VALUES(id),created_at=VALUES(created_at),updated_at=VALUES(updated_at)"
	want = fmt.Sprintf(want,
		model.ID,
		model.CreatedAt.Format(timeFmt),
		model.UpdatedAt.Format(timeFmt))
	assert.Equal(t, want, have)
}

func TestCraftCreateManyUpdate(t *testing.T) {
	{
		models := newTest()
		_, err := craftCreateManyUpdate(&Model{models})
		assert.Error(t, err)

	}
	{
		{
			models := newTestSlice()

			have, err := craftCreateManyUpdate(&Model{models})
			if err != nil {
				t.Error((err))
			}
			want := `INSERT INTO test (id,created_at,updated_at) VALUES('%s','%s','%s'),('%s','%s','%s')ON DUPLICATE KEY UPDATE id=VALUES(id),created_at=VALUES(created_at),updated_at=VALUES(updated_at)`
			want = fmt.Sprintf(want,
				models[0].ID,
				models[0].CreatedAt.Format(timeFmt),
				models[0].UpdatedAt.Format(timeFmt),
				models[1].ID,
				models[1].CreatedAt.Format(timeFmt),
				models[1].UpdatedAt.Format(timeFmt),
			)

			assert.Equal(t, want, have)
		}
		{
			models := newTestSlice()
			have, err := craftCreateManyUpdate(&Model{&models})
			if err != nil {
				t.Error((err))
			}
			want := `INSERT INTO test (id,created_at,updated_at) VALUES('%s','%s','%s'),('%s','%s','%s')ON DUPLICATE KEY UPDATE id=VALUES(id),created_at=VALUES(created_at),updated_at=VALUES(updated_at)`
			want = fmt.Sprintf(
				want,
				models[0].ID,
				models[0].CreatedAt.Format(timeFmt),
				models[0].UpdatedAt.Format(timeFmt),
				models[1].ID,
				models[1].CreatedAt.Format(timeFmt),
				models[1].UpdatedAt.Format(timeFmt),
			)

			assert.Equal(t, want, have)
		}
	}
}

func TestCraftCreateManyTemp(t *testing.T) {
	{
		models := newTest()
		_, err := craftCreateManyTemp(&Model{models})
		assert.Error(t, err)

	}
	{
		{
			models := newTestSlice()

			have, err := craftCreateManyTemp(&Model{models})
			if err != nil {
				t.Error((err))
			}
			want := `INSERT INTO test_TEMP (id,created_at,updated_at) VALUES('%s','%s','%s'),('%s','%s','%s')`
			want = fmt.Sprintf(want,
				models[0].ID,
				models[0].CreatedAt.Format(timeFmt),
				models[0].UpdatedAt.Format(timeFmt),
				models[1].ID,
				models[1].CreatedAt.Format(timeFmt),
				models[1].UpdatedAt.Format(timeFmt),
			)

			assert.Equal(t, want, have)
		}
		{
			models := newTestSlice()
			have, err := craftCreateManyTemp(&Model{&models})
			if err != nil {
				t.Error((err))
			}
			want := `INSERT INTO test_TEMP (id,created_at,updated_at) VALUES('%s','%s','%s'),('%s','%s','%s')`
			want = fmt.Sprintf(
				want,
				models[0].ID,
				models[0].CreatedAt.Format(timeFmt),
				models[0].UpdatedAt.Format(timeFmt),
				models[1].ID,
				models[1].CreatedAt.Format(timeFmt),
				models[1].UpdatedAt.Format(timeFmt),
			)

			assert.Equal(t, want, have)
		}
	}
}

func TestGenericDestroyMany(t *testing.T) {
	err := genericDestroyMany(badDB{}, &Model{newTestSlice()})
	assert.NotEqual(t, nil, err)
}

func TestGenericSelectOne(t *testing.T) {
	err := genericSelectOne(badDB{}, &Model{newTest()}, Query{RawSQL: &clause{}, Connection: &Connection{Dialect: &mysql{}}})
	assert.NotEqual(t, nil, err)
}
func TestGenericSelectMany(t *testing.T) {
	err := genericSelectMany(badDB{}, &Model{newTest()}, Query{RawSQL: &clause{}, Connection: &Connection{Dialect: &mysql{}}})
	assert.NotEqual(t, nil, err)
}
func TestGenericSQLVIew(t *testing.T) {
	err := genericSQLView(badDB{}, &Model{newTest()}, nil)
	assert.NotEqual(t, nil, err)
}
func TestGenericCreateUpdate(t *testing.T) {
	err := genericCreateUpdate(badDB{}, &Model{newTest()})
	assert.NotEqual(t, nil, err)
}
func TestGenericCreateManyUpdate(t *testing.T) {
	err := genericCreateManyUpdate(badDB{}, &Model{newTest()})
	assert.NotEqual(t, nil, err)
}

func TestGenericCreateManyTemp(t *testing.T) {
	err := genericCreateManyTemp(badDB{}, &Model{newTest()})
	assert.NotEqual(t, nil, err)
}
func TestGenericCreateMany(t *testing.T) {
	err := genericCreateMany(badDB{}, &Model{newTest()})
	assert.NotEqual(t, nil, err)
}
func TestGenericUpdate(t *testing.T) {
	err := genericUpdate(badDB{}, &Model{newTest()})
	assert.NotEqual(t, nil, err)
}
func TestGenericDestroy(t *testing.T) {
	err := genericDestroy(badDB{}, &Model{newTest()})
	assert.NotEqual(t, nil, err)
}
func TestGenericCreate(t *testing.T) {
	err := genericCreate(badDB{}, &Model{newTest()})
	assert.NotEqual(t, nil, err)
}

func TestGenericTruncate(t *testing.T) {
	err := genericTruncate(badDB{}, &Model{newTest()})
	assert.NotEqual(t, nil, err)
}

func TestGenericExec(t *testing.T) {
	c := &Connection{DB: &badDB{}}

	err := genericExec(c.DB, "")
	assert.NotEqual(t, nil, err)
}
