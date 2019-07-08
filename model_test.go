package dasorm

import (
	"fmt"
	"testing"

	uuid "github.com/satori/go.uuid"
)

func TestModelID(t *testing.T) {
	m := Model{NewTestStruct()}
	if want, have := testUUID, m.ID().(uuid.UUID); want != have {
		t.Errorf("have: %v, want: %v", have, want)
	}
}
func TestModelTableName(t *testing.T) {
	m := Model{NewTestStruct()}
	if want, have := "test", m.TableName(); want != have {
		t.Errorf("have: %v, want: %v", have, want)
	}
}

func TestModelTouchCreatedAt(t *testing.T) {
	test := NewTestStruct()
	m := Model{test}
	m.touchCreatedAt()
	if want, have := test.CreatedAt, testTime; want.Equal(have) {
		t.Error("times are equal still...")
	}
}
func TestModelTouchUpdatedAt(t *testing.T) {
	test := NewTestStruct()
	m := Model{test}
	m.touchUpdatedAt()
	if want, have := test.UpdatedAt, testTime; want.Equal(have) {
		t.Error("times are equal still...")
	}
}

func TestModelWhereID(t *testing.T) {
	m := Model{NewTestStruct()}
	want := fmt.Sprintf("id='%s'", testUUID.String())
	if have := m.whereID(); want != have {
		t.Errorf("have: %v, want: %v", have, want)
	}
}

func TestModelIsSlice(t *testing.T) {
	m := Model{NewTestStruct()}
	if m.isSlice() {
		t.Error("should not be slice...")
	}
	m = Model{
		[]*TestStruct{
			NewTestStruct(),
			NewTestStruct(),
		},
	}
	if !m.isSlice() {
		t.Error("model should be slice...")
	}
}

func TestModelUpdateString(t *testing.T) {
	m := Model{NewTestStruct()}
	have := "name = :name, updated_at = :updated_at, an_int = :an_int, a_float = :a_float, a_bool = :a_bool"
	if want := m.UpdateString(); want != have {
		t.Errorf("have: %v, want: %v", have, want)
	}
}
