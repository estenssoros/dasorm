package dasorm

import (
	"fmt"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
)

type TestStruct struct {
	ID        uuid.UUID `db:"id"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`
	AnInt     int       `db:"an_int"`
	AFloat    float64   `db:"a_float"`
	ABool     bool      `db:"a_bool"`
}

var (
	testUUID   = uuid.Must(uuid.FromString("86f65f0c-0320-461b-9047-6303d79db43d"))
	testTime   = time.Now()
	testFormat map[string]string
)

func init() {
	testFormat = map[string]string{
		"test_uuid": testUUID.String(),
		"test_time": testTime.Format("2006-01-02 15:04:05"),
	}
}

func (t *TestStruct) TableName() string {
	return `test`
}
func NewTestStruct() *TestStruct {
	return &TestStruct{
		ID:        testUUID,
		Name:      "asdf",
		CreatedAt: testTime,
		AnInt:     7,
		AFloat:    7.0,
		ABool:     true,
	}
}

func TestMapToStruct(t *testing.T) {
	m := map[string]interface{}{
		"ID":        testUUID,
		"Name":      "asdf",
		"CreatedAt": testTime,
		"AnInt":     7,
		"AFloat":    7.0,
		"ABool":     true,
	}
	v := &TestStruct{}
	if err := MapToStruct(v, m); err != nil {
		t.Fatal(err)
	}
	fmt.Println(v)
}

func TestInsertStmt(t *testing.T) {
	m := NewTestStruct()
	want := "INSERT INTO test (id,name,created_at,an_int,a_float,a_bool) VALUES "
	if have := InsertStmt(m); want != have {
		t.Errorf("have: %s, want: %s", have, want)
	}
}

func TestReplaceStmt(t *testing.T) {
	m := NewTestStruct()
	want := "REPLACE INTO test (id,name,created_at,an_int,a_float,a_bool) VALUES"
	if have := ReplaceStmt(m); want != have {
		t.Errorf("have: %s, want: %s", have, want)
	}
}

func TestSelectStmt(t *testing.T) {
	m := NewTestStruct()
	want := "SELECT id,name,created_at,an_int,a_float,a_bool FROM test"
	if have := SelectStmt(m); want != have {
		t.Errorf("have: %s, want: %s", have, want)
	}
}
func TestTruncateStmt(t *testing.T) {
	m := NewTestStruct()
	want := "TRUNCATE TABLE test"
	if have := TruncateStmt(m); want != have {
		t.Errorf("have: %s, want: %s", have, want)
	}
}
func TestInsertIgnoreStmt(t *testing.T) {
	m := NewTestStruct()
	want := "INSERT IGNORE INTO test (id,name,created_at,an_int,a_float,a_bool) VALUES"
	if have := InsertIgnoreStmt(m); want != have {
		t.Errorf("have: %s, want: %s", have, want)
	}
}

func TestStringTuple(t *testing.T) {
	m := NewTestStruct()
	want := "('{test_uuid}','asdf','{test_time}',7,7.000000,true)"
	want = MustFormatMap(want, testFormat)
	if have := StringTuple(m); want != have {
		t.Errorf("have: %s, want: %s", have, want)
	}
}
func TestStringSlice(t *testing.T) {
	m := NewTestStruct()
	wantSlice := []string{
		fmt.Sprintf("%s", testUUID.String()),
		"asdf",
		fmt.Sprintf("%s", testTime.Format(timeFmt)),
		"7",
		"7.000000",
		"true",
	}
	haveSlice := StringSlice(m)
	if want, have := len(wantSlice), len(haveSlice); want != have {
		t.Errorf("have: %d, want: %d", want, have)
	}
	for i := 0; i < len(haveSlice); i++ {
		if want, have := wantSlice[i], haveSlice[i]; want != have {
			t.Errorf("have: %s, want: %s", want, have)
		}
	}
}

func TestCSVHeaders(t *testing.T) {
	m := NewTestStruct()
	wantHeaders := []string{
		"id",
		"name",
		"created_at",
		"an_int",
		"a_float",
		"a_bool",
	}
	haveHeaders := CSVHeaders(m)
	if want, have := len(wantHeaders), len(haveHeaders); want != have {
		t.Errorf("have: %d, want: %d", want, have)
	}
	for i := 0; i < len(haveHeaders); i++ {
		if want, have := wantHeaders[i], haveHeaders[i]; want != have {
			t.Errorf("have: %s, want: %s", want, have)
		}
	}
}
