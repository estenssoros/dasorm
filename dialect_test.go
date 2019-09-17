package dasorm

import (
	"fmt"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

type test struct {
	ID        uuid.UUID `db:"id"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

func (t test) SQLView() string {
	return `howdy {name}`
}

var (
	defaultTime = time.Now()
	defaultUUID = uuid.Must(uuid.NewV4())
)

func TestCraftCreate(t *testing.T) {
	model := &test{
		ID:        defaultUUID,
		CreatedAt: defaultTime,
		UpdatedAt: defaultTime,
	}
	have := craftCreate(&Model{model})
	want := "INSERT INTO test (id,created_at,updated_at) VALUES('%s','%s','%s')"
	want = fmt.Sprintf(want, model.ID, model.CreatedAt.Format(timeFmt), model.UpdatedAt.Format(timeFmt))
	assert.Equal(t, want, have)
}

func TestCraftCreateMany(t *testing.T) {
	{
		models := &test{
			ID:        defaultUUID,
			CreatedAt: defaultTime,
			UpdatedAt: defaultTime,
		}
		_, err := craftCreateMany(&Model{models})
		assert.Error(t, err)

	}
	{
		models := []*test{
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
	model := &test{
		ID:        defaultUUID,
		CreatedAt: defaultTime,
		UpdatedAt: defaultTime,
	}
	have := craftUpdate(&Model{model})
	want := "UPDATE test SET updated_at = :updated_at WHERE id='%s'"
	want = fmt.Sprintf(want, model.ID)
	assert.Equal(t, want, have)
}

func TestCraftDestroy(t *testing.T) {
	model := &test{
		ID:        defaultUUID,
		CreatedAt: defaultTime,
		UpdatedAt: defaultTime,
	}
	have := craftDestroy(&Model{model})
	want := "DELETE FROM test WHERE id='%s'"
	want = fmt.Sprintf(want, model.ID)
	assert.Equal(t, want, have)
}

func TestCraftDestroyMany(t *testing.T) {
	{
		model :=
			&test{
				ID: defaultUUID,
			}
		_, err := craftDestroyMany(&Model{model})
		if err == nil {
			t.Error("should error")
		}
	}
	{
		models := []*test{
			&test{
				ID: defaultUUID,
			},
			&test{
				ID: defaultUUID,
			},
		}
		have, err := craftDestroyMany(&Model{models})
		if err != nil {
			t.Error(err)
		}
		want := `DELETE FROM test WHERE id IN ('%s','%s')`
		want = fmt.Sprintf(want, defaultUUID, defaultUUID)
		assert.Equal(t, want, have)
	}
	{
		models := []test{
			test{
				ID: defaultUUID,
			},
			test{
				ID: defaultUUID,
			},
		}
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
	model := &test{}
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
		models := &test{
			ID:        defaultUUID,
			CreatedAt: defaultTime,
			UpdatedAt: defaultTime,
		}
		_, err := craftCreateManyUpdate(&Model{models})
		assert.Error(t, err)

	}
	{
		{
			models := []*test{
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
			models := []test{
				test{
					ID:        defaultUUID,
					CreatedAt: defaultTime,
					UpdatedAt: defaultTime,
				},
				test{
					ID:        defaultUUID,
					CreatedAt: defaultTime,
					UpdatedAt: defaultTime,
				},
			}
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
		models := &test{
			ID:        defaultUUID,
			CreatedAt: defaultTime,
			UpdatedAt: defaultTime,
		}
		_, err := craftCreateManyTemp(&Model{models})
		assert.Error(t, err)

	}
	{
		{
			models := []*test{
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
			models := []test{
				test{
					ID:        defaultUUID,
					CreatedAt: defaultTime,
					UpdatedAt: defaultTime,
				},
				test{
					ID:        defaultUUID,
					CreatedAt: defaultTime,
					UpdatedAt: defaultTime,
				},
			}
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
