package dasorm

import (
	"fmt"
	"testing"
	"time"

	"github.com/estenssoros/dasorm/nulls"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

type TestStruct struct {
	ID        uuid.UUID `db:"id"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
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
		UpdatedAt: testTime,
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
		"UpdatedAt": testTime,
		"AnInt":     7,
		"AFloat":    7.0,
		"ABool":     true,
	}
	v := &TestStruct{}
	if err := MapToStruct(v, m); err != nil {
		t.Error(err)
	}
}

func TestInsertStmt(t *testing.T) {
	m := NewTestStruct()
	want := "INSERT INTO test (id,name,created_at,updated_at,an_int,a_float,a_bool) VALUES"
	if have := InsertStmt(m); want != have {
		t.Errorf("have: %s, want: %s", have, want)
	}
}

func TestReplaceStmt(t *testing.T) {
	m := NewTestStruct()
	want := "REPLACE INTO test (id,name,created_at,updated_at,an_int,a_float,a_bool) VALUES"
	if have := ReplaceStmt(m); want != have {
		t.Errorf("have: %s, want: %s", have, want)
	}
}

func TestSelectStmt(t *testing.T) {
	m := NewTestStruct()
	want := "SELECT id,name,created_at,updated_at,an_int,a_float,a_bool FROM test"
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
	want := "INSERT IGNORE INTO test (id,name,created_at,updated_at,an_int,a_float,a_bool) VALUES"
	if have := InsertIgnoreStmt(m); want != have {
		t.Errorf("have: %s, want: %s", have, want)
	}
}

func TestStringTuple(t *testing.T) {
	m := NewTestStruct()
	want := "('{test_uuid}','asdf','{test_time}','{test_time}',7,7.000000,true)"
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
		"updated_at",
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

func TestIsErrorNoRows(t *testing.T) {
	assert.Equal(t, true, IsErrorNoRows(errors.New("no rows in result set")))
}

func TestEscapeString(t *testing.T) {
	testString := `0\n\r\\\'\032`
	assert.Equal(t, `0\\n\\r\\\\\\\'\\032`, EscapeString(testString))
}

func TestFieldTypeNulls(t *testing.T) {
	test := struct {
		I nulls.Int
		S nulls.String
		F nulls.Float64
		T nulls.Time
		B nulls.Bool
	}{
		I: nulls.Int{},
		S: nulls.String{},
		F: nulls.Float64{},
		T: nulls.Time{},
		B: nulls.Bool{},
	}
	have := StringSlice(test)
	assert.Equal(t, []string{"NULL", "NULL", "NULL", "NULL", "NULL"}, have)
}

func TestFieldTypeNullsValid(t *testing.T) {
	now := time.Now()
	test := struct {
		I nulls.Int
		S nulls.String
		F nulls.Float64
		T nulls.Time
		B nulls.Bool
	}{
		I: nulls.NewInt(1),
		S: nulls.NewString("asdf"),
		F: nulls.NewFloat64(1),
		T: nulls.NewTime(now),
		B: nulls.NewBool(true),
	}
	have := StringSlice(test)
	assert.Equal(t, []string{"1", "asdf", "1.000000", now.Format(timeFmt), "1"}, have)
}

type testStuct001 struct {
	ID          uuid.UUID    `json:"id" db:"id"`
	IMO         nulls.Int    `json:"imo" db:"imo"`
	CreatedAt   time.Time    `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at" db:"updated_at"`
	CaseID      uuid.UUID    `json:"case_id" db:"case_id"`
	Name        nulls.String `json:"name" db:"name"`
	Year        nulls.Int    `json:"year" db:"year"`
	Assumed     nulls.Int    `json:"assumed" db:"assumed"`
	AddDays     nulls.Int    `json:"add_days" db:"add_days"`
	AddDate     nulls.Time   `json:"add_date" db:"add_date"`
	AddNotes    nulls.String `json:"add_notes" db:"add_notes"`
	Rate        nulls.Int    `json:"rate,omitempty"`
	Utilization nulls.Int    `json:"utilization,omitempty"`
	SpotID      uuid.UUID    `json:"spot_id"`
	OffhireID   uuid.UUID    `json:"offhire_id"`
}

func TestStruct001(t *testing.T) {
	id1 := uuid.Must(uuid.NewV4())
	id2 := uuid.Must(uuid.NewV4())
	ts := time.Now()
	t001 := &testStuct001{
		CaseID:      id2,
		ID:          id1,
		Name:        nulls.NewString("asdf"),
		Year:        nulls.NewInt(ts.Year()),
		IMO:         nulls.NewInt(69),
		Assumed:     nulls.NewInt(7),
		AddDays:     nulls.NewInt(7),
		AddDate:     nulls.NewTime(ts),
		AddNotes:    nulls.NewString("string"),
		Rate:        nulls.NewInt(7),
		Utilization: nulls.NewInt(7),
	}
	insert := `INSERT INTO testStuct001 (id,imo,created_at,updated_at,case_id,name,year,assumed,add_days,add_date,add_notes) VALUES`
	assert.Equal(t, insert, InsertStmt(t001))
	values := `('%s',69,'0001-01-01 00:00:00','0001-01-01 00:00:00','%s','asdf',2019,7,7,'%s','string')`
	values = fmt.Sprintf(values, id1, id2, ts.Format(timeFmt))
	assert.Equal(t, values, StringTuple(t001))
}
