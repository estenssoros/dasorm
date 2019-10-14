package dasorm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModelToColumn(t *testing.T) {
	{
		m := &Model{NewTestStruct()}
		cols := m.ToColumns()
		assert.Equal(t, 7, len(cols))
	}
	{
		ts := NewTestStruct()
		m := &Model{*ts}
		cols := m.ToColumns()
		assert.Equal(t, 7, len(cols))
	}
}

func TestSQLView(t *testing.T) {
	m := &Model{NewTestSlice()}
	out, err := m.SQLView()
	assert.Equal(t, nil, err)
	fmt.Println(out)
}
func fn(m *Model) error    { return nil }
func fnerr(m *Model) error { return errOnPurpose }
func TestModelIterate(t *testing.T) {
	{
		m := &Model{NewTestSlice()}
		assert.Equal(t, nil, m.iterate(fn))
		assert.Error(t, m.iterate(fnerr))
	}
	{
		m := &Model{NewTestStruct()}
		assert.Equal(t, nil, m.iterate(fn))
		assert.Error(t, m.iterate(fnerr))
	}
}

func TestColumnSliceSafe(t *testing.T) {
	m := &Model{NewTestStruct()}
	want := []string{"`id`", "`name`", "`created_at`", "`updated_at`", "`an_int`", "`a_float`", "`a_bool`"}
	assert.Equal(t, want, m.ColumnSliceSafe())
}

func TestColumnsSafe(t *testing.T) {
	m := &Model{NewTestStruct()}
	want := "`id`,`name`,`created_at`,`updated_at`,`an_int`,`a_float`,`a_bool`"
	assert.Equal(t, want, m.ColumnsSafe())
}

func TestTokenizedString(t *testing.T) {
	m := &Model{NewTestStruct()}
	want := ":id, :name, :created_at, :updated_at, :an_int, :a_float, :a_bool"
	assert.Equal(t, want, m.TokenizedString())
}
