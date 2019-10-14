package dasorm

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/estenssoros/dasorm/nulls"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

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

type TestStruct struct {
	ID        uuid.UUID `db:"id"`
	Name      string    `db:"name" filter:"asdf"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
	AnInt     int       `db:"an_int"`
	AFloat    float64   `db:"a_float"`
	ABool     bool      `db:"a_bool"`
}

func (t TestStruct) TableName() string {
	return `test`
}

func (t TestStruct) SQLView() string {
	return `howdy {name}`
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
func (t TestStruct) String() string {
	ju, _ := json.Marshal(t)
	return string(ju)
}

func NewTestSlice() []*TestStruct {
	slice := make([]*TestStruct, 5)
	for i := 0; i < 5; i++ {
		slice[i] = NewTestStruct()
	}
	return slice
}

type TestNullsStruct struct {
	Name      nulls.String  `db:"name"`
	CreatedAt nulls.Time    `db:"created_at"`
	Abool     nulls.Bool    `db:"abool"`
	AFloat    nulls.Float64 `db:"a_float"`
	AnInt     nulls.Int     `db:"an_int"`
}

func NewNullsStruct() *TestNullsStruct {
	return &TestNullsStruct{
		Name:      nulls.NewString("asdf"),
		CreatedAt: nulls.NewTime(testTime),
		Abool:     nulls.NewBool(true),
		AFloat:    nulls.NewFloat64(7.0),
		AnInt:     nulls.NewInt(7),
	}
}
func NewNullsStructNull() *TestNullsStruct {
	return &TestNullsStruct{
		Name:      nulls.String{},
		CreatedAt: nulls.Time{},
		Abool:     nulls.Bool{},
		AFloat:    nulls.Float64{},
		AnInt:     nulls.Int{},
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
	want := "('{test_uuid}','asdf','{test_time}','{test_time}',7,7,true)"
	want = MustFormatMap(want, testFormat)

	assert.Equal(t, want, StringTuple(m))
}
func TestStringSlice(t *testing.T) {
	m := NewTestStruct()
	wantSlice := []string{
		fmt.Sprintf("%s", testUUID.String()),
		"asdf",
		fmt.Sprintf("%s", testTime.Format(timeFmt)),
		fmt.Sprintf("%s", testTime.Format(timeFmt)),
		"7",
		"7",
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
func TestCSVHeadersConnection(t *testing.T) {
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
	c := &Connection{}
	haveHeaders := c.CSVHeaders(m)
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

var escapeTests = []struct {
	in  string
	out string
}{
	{"\n", "\\n"},
	{"\r", "\\r"},
	{"\\", "\\\\"},
	{`\'`, `\\\'`},
	{`"`, `\"`},
}

func TestEscapeString(t *testing.T) {
	for i, tt := range escapeTests {
		assert.Equalf(t, tt.out, EscapeString(tt.in), "test %d", i+1)
	}
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
	{
		have := StringSlice(test)
		assert.Equal(t, []string{"NULL", "NULL", "NULL", "NULL", "NULL"}, have)
	}
	{
		c := &Connection{}
		have := c.StringSlice(test)
		assert.Equal(t, []string{"NULL", "NULL", "NULL", "NULL", "NULL"}, have)
	}
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
	assert.Equal(t, []string{"1", "asdf", "1", now.Format(timeFmt), "1"}, have)
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

type TestStruct002 struct {
	ID         uuid.UUID
	Time       time.Time
	Name       string
	IMO        int
	Value      float64
	Abool      bool
	NullInt    nulls.Int
	NullString nulls.String
	NullFloat  nulls.Float64
	NullTime   nulls.Time
	NullBool   nulls.Bool
}

func TestDecodeSlice(t *testing.T) {
	now := time.Now()
	id := uuid.Must(uuid.NewV4())
	t002 := &TestStruct002{}
	d := []string{
		id.String(),
		now.Format(time.RFC3339),
		"Seaspan Chiwan",
		"91234991",
		"131234.4321",
		"1",
		"",
		"",
		"",
		"",
		"",
	}
	assert.Equal(t, nil, DecodeSlice(d, t002))
	assert.Equal(t, t002.IMO, 91234991)
	assert.Equal(t, t002.ID, id)
	assert.Equal(t, t002.Value, 131234.4321)
	assert.Equal(t, t002.Abool, true)
}

func TestStringSliceFilter(t *testing.T) {
	ts := NewTestStruct()
	{
		ss := StringSliceFilter(ts, "filter")
		assert.Equal(t, 1, len(ss))
	}
	{
		ss := StringSliceFilter(ts, nil)
		assert.Equal(t, 7, len(ss))
	}
}

func TestStringTupleNull(t *testing.T) {
	{
		ts := NewNullsStruct()
		st := StringTuple(ts)
		assert.Equal(t, fmt.Sprintf("('asdf','%s',1,7,7)", testTime.Format(timeFmt)), st)
	}
	{
		ts := NewNullsStruct()
		ts.Abool = nulls.NewBool(false)
		st := StringTuple(ts)
		assert.Equal(t, fmt.Sprintf("('asdf','%s',0,7,7)", testTime.Format(timeFmt)), st)
	}
	{
		ts := NewNullsStruct()
		ts.AFloat = nulls.NewFloat64(math.NaN())
		st := StringTuple(ts)
		assert.Equal(t, fmt.Sprintf("('asdf','%s',1,NULL,7)", testTime.Format(timeFmt)), st)
	}
	{
		ts := NewNullsStructNull()
		st := StringTuple(ts)
		assert.Equal(t, "(NULL,NULL,NULL,NULL,NULL)", st)
	}
}
func TestColumns(t *testing.T) {
	ts := NewTestStruct()
	cs := Columns(ts)
	assert.Equal(t, []string{"id", "name", "created_at", "updated_at", "an_int", "a_float", "a_bool"}, cs)
}

func TestStructHeaders(t *testing.T) {
	ts := NewTestStruct()
	assert.Equal(t, []string{"ID", "Name", "CreatedAt", "UpdatedAt", "AnInt", "AFloat", "ABool"}, StructHeaders(ts))

}

func TestTableName(t *testing.T) {
	ts := NewTestStruct()
	assert.Equal(t, "test", TableName(ts))
}

var schema = "\"ID\" VARCHAR(54)\n, \"NAME\" VARCHAR(6)\n, \"CREATED_AT\" DATETIME\n, \"UPDATED_AT\" DATETIME\n, \"AN_INT\" INTEGER\n, \"A_FLOAT\" FLOAT\n, \"A_BOOL\" BOOLEAN"

func TestCreateSchemaSlice(t *testing.T) {
	ts := NewTestSlice()
	assert.Equal(t, schema, createSchemaSlice(&Model{ts}))
	assert.Equal(t, schema, CreateSchema(ts))
	nonPointers := make([]TestStruct, len(ts))
	for i, _t := range ts {
		nonPointers[i] = *_t
	}
	assert.Equal(t, schema, createSchemaSlice(&Model{nonPointers}))
}

func TestCreateSchemaSingleton(t *testing.T) {
	ts := NewTestStruct()
	assert.Equal(t, schema, createSchemaSingleton(&Model{ts}))
	assert.Equal(t, schema, CreateSchema(ts))
}

func TestToTuples(t *testing.T) {
	ts := NewTestSlice()
	tuples, err := ToTuples(ts)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(ts), len(tuples))
	noPointers := make([]TestStruct, len(ts))
	for i, _t := range ts {
		noPointers[i] = *_t
	}
	tuples, err = ToTuples(noPointers)
	assert.Equal(t, nil, err)
	assert.Equal(t, len(noPointers), len(tuples))
}

var snakeCaseTests = []struct {
	in  string
	out string
}{
	{"asdf", "asdf"},
	{"ASDF", "asdf"},
	{"AsDf", "as_df"},
}

func TestToSnakeCase(t *testing.T) {
	for _, tt := range snakeCaseTests {
		have := ToSnakeCase(tt.in)
		assert.Equal(t, have, tt.out)
	}
}
