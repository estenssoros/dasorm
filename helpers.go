package dasorm

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/estenssoros/dasorm/nulls"
	interpol "github.com/imkira/go-interpol"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

var timeFmt = "2006-01-02 15:04:05"

// IsErrorNoRows determine if the error is no rows in result
func IsErrorNoRows(err error) bool {
	return strings.Contains(err.Error(), "no rows in result set")
}

// EscapeString replaces error causing characters in  a string
func EscapeString(sql string) string {
	dest := make([]byte, 0, 2*len(sql))
	var escape byte
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		escape = 0
		switch c {
		case 0: /* Must be escaped for 'mysql' */
			escape = '0'
		case '\n': /* Must be escaped for logs */
			escape = 'n'
		case '\r':
			escape = 'r'
		case '\\':
			escape = '\\'
		case '\'':
			escape = '\''
		case '"': /* Better safe than sorry */
			escape = '"'
		case '\032': /* This gives problems on Win32 */
			escape = 'Z'
		}
		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, c)
		}
	}
	return string(dest)
}

// StringSlice converts all fields of a struct to a string slice
func (c *Connection) StringSlice(v interface{}) []string {
	return StringSlice(v)
}

const (
	// StringKind kind
	StringKind = 0
	// IntKind kind
	IntKind = iota
	// FloatKind kind
	FloatKind = iota
	// BoolKind kind
	BoolKind = iota
	// OtherKind kind
	OtherKind = iota
	// TimeType kind
	TimeType = iota
	// UUIDType kind
	UUIDType = iota
	// NullsIntType kind
	NullsIntType = iota
	// NullsStringType kind
	NullsStringType = iota
	// NullsFloatType kind
	NullsFloatType = iota
	// NullsTimeType kind
	NullsTimeType = iota
	// NullsBoolType kind
	NullsBoolType = iota
	// SQLNullInt64Type kind from sql package
	SQLNullInt64Type = iota
	// SQLNullFloat64Type kind from sql package
	SQLNullFloat64Type = iota
	// SQLNullStringType kind from sql package
	SQLNullStringType = iota
	// SQLNullBoolType kind from sql package
	SQLNullBoolType = iota
	// OtherType kind
	OtherType = iota
)

// ValueKind determines the value kind of a reflect value
func ValueKind(v reflect.Value) int {
	switch v.Kind() {
	case reflect.String:
		return StringKind
	case reflect.Int, reflect.Int16, reflect.Int8, reflect.Int32, reflect.Int64:
		return IntKind
	case reflect.Float64, reflect.Float32:
		return FloatKind
	case reflect.Bool:
		return BoolKind
	default:
		return OtherKind
	}
}

// ValueToString converts a reflect value to string based on the kind
func ValueToString(v reflect.Value, kind int) string {
	switch kind {
	case StringKind:
		return v.String()
	case IntKind:
		return fmt.Sprintf("%d", v.Int())
	case FloatKind:
		if f := v.Float(); math.IsNaN(f) {
			return "NULL"
		}
		return fmt.Sprintf("%f", v.Float())
	case BoolKind:
		return fmt.Sprintf("%v", v.Bool())
	default:
		return ""
	}
}

// FieldType finds the special field type of a field
func FieldType(f reflect.StructField) int {
	switch f.Type {
	case reflect.TypeOf(time.Time{}):
		return TimeType
	case reflect.TypeOf(uuid.UUID{}):
		return UUIDType
	case reflect.TypeOf(nulls.Int{}):
		return NullsIntType
	case reflect.TypeOf(nulls.String{}):
		return NullsStringType
	case reflect.TypeOf(nulls.Float64{}):
		return NullsFloatType
	case reflect.TypeOf(nulls.Time{}):
		return NullsTimeType
	case reflect.TypeOf(nulls.Bool{}):
		return NullsBoolType
	case reflect.TypeOf(sql.NullInt64{}):
		return SQLNullInt64Type
	case reflect.TypeOf(sql.NullFloat64{}):
		return SQLNullFloat64Type
	case reflect.TypeOf(sql.NullString{}):
		return SQLNullStringType
	case reflect.TypeOf(sql.NullBool{}):
		return SQLNullBoolType
	default:
		return OtherType
	}
}

// FieldToString converts a reflect values to string based on field type
func FieldToString(v reflect.Value, fType int) string {
	i := v.Interface()
	switch fType {
	case TimeType:
		return i.(time.Time).Format(timeFmt)
	case UUIDType:
		return i.(uuid.UUID).String()
	case NullsIntType:
		if v := i.(nulls.Int); v.Valid {
			return fmt.Sprintf("%d", v.Int)
		}
		return "NULL"
	case NullsStringType:
		if v := i.(nulls.String); v.Valid {
			return fmt.Sprintf("%s", v.String)
		}
		return "NULL"
	case NullsFloatType:
		if v := i.(nulls.Float64); v.Valid {
			return fmt.Sprintf("%f", v.Float64)
		}
		return "NULL"
	case NullsTimeType:
		if v := i.(nulls.Time); v.Valid {
			return v.Time.Format("'2006-01-02 15:04:05'")
		}
		return "NULL"
	case NullsBoolType:
		if v := i.(nulls.Bool); v.Valid {
			if v.Bool {
				return "1"
			}
			return "0"
		}
		return "NULL"
	}
	return "NULL"
}

// StringSliceFilter attemps to only filter for a certain struct field
func StringSliceFilter(v, f interface{}) []string {
	var filter string
	if f == nil {
		return StringSlice(v)
	}

	filter = f.(string)

	fields := reflect.TypeOf(v)
	values := reflect.ValueOf(v)
	if values.Kind() == reflect.Ptr {
		values = values.Elem()
		fields = fields.Elem()
	}
	numFields := fields.NumField()
	stringSlice := []string{}
	for i := 0; i < numFields; i++ {
		field := fields.Field(i)
		if tag := field.Tag.Get(filter); tag == "" {
			continue
		}
		value := values.Field(i)
		kind := ValueKind(value)
		switch kind {
		case StringKind, IntKind, FloatKind, BoolKind:
			stringSlice = append(stringSlice, ValueToString(value, kind))
		case OtherKind:
			fType := FieldType(field)
			switch fType {
			case TimeType, UUIDType, NullsIntType, NullsStringType, NullsFloatType, NullsTimeType, NullsBoolType:
				stringSlice = append(stringSlice, FieldToString(value, fType))
			case OtherType:
				panic(fmt.Sprintf("unknown field type: %v", field.Type))
			}
		}
	}
	return stringSlice
}

// StringSlice converts all fields of a struct to a string slice
func StringSlice(v interface{}) []string {
	fields := reflect.TypeOf(v)
	values := reflect.ValueOf(v)
	if values.Kind() == reflect.Ptr {
		values = values.Elem()
		fields = fields.Elem()
	}
	numFields := fields.NumField()
	stringSlice := make([]string, numFields)
	for i := 0; i < numFields; i++ {
		field := fields.Field(i)
		value := values.Field(i)
		kind := ValueKind(value)
		switch kind {
		case StringKind, IntKind, FloatKind, BoolKind:
			stringSlice[i] = ValueToString(value, kind)
		case OtherKind:
			fType := FieldType(field)
			switch fType {
			case TimeType, UUIDType, NullsIntType, NullsStringType, NullsFloatType, NullsTimeType, NullsBoolType:
				stringSlice[i] = FieldToString(value, fType)
			case OtherType:
				panic(fmt.Sprintf("unknown field type: %v", field.Type))
			}
		}
	}
	return stringSlice
}

// MapToStruct converts a map of string interface to struct
func MapToStruct(v interface{}, m map[string]interface{}) error {
	values := reflect.ValueOf(v)
	if values.Kind() != reflect.Ptr {
		return errors.New("map to string only supports pointers. passed non-pointer value")
	}
	values = values.Elem()
	for name, i := range m {
		fbn := values.FieldByName(name)
		if !fbn.IsValid() {
			continue
		}
		fbn.Set(reflect.ValueOf(i))
	}
	return nil
}

// StringTuple converts struct to MySQL compatible string tuple
func StringTuple(c interface{}) string {
	fields := reflect.TypeOf(c)
	values := reflect.ValueOf(c)
	if values.Kind() == reflect.Ptr {
		values = values.Elem()
		fields = fields.Elem()
	}
	numFields := fields.NumField()
	stringSlice := []string{}
	for i := 0; i < numFields; i++ {
		field := fields.Field(i)
		value := values.Field(i)
		if tag := field.Tag.Get("db"); tag == "" {
			continue
		}
		kind := ValueKind(value)
		switch kind {
		case StringKind:
			stringSlice = append(stringSlice, fmt.Sprintf("'%s'", EscapeString(value.String())))
		case IntKind, FloatKind, BoolKind:
			stringSlice = append(stringSlice, ValueToString(value, kind))
		case OtherKind:
			fType := FieldType(field)
			switch fType {
			case TimeType, UUIDType:
				stringSlice = append(stringSlice, fmt.Sprintf("'%s'", FieldToString(value, fType)))
			case NullsIntType:
				v := value.Interface().(nulls.Int)
				if v.Valid {
					stringSlice = append(stringSlice, fmt.Sprintf("%d", v.Int))
				} else {
					stringSlice = append(stringSlice, "NULL")
				}
			case NullsStringType:
				if v := value.Interface().(nulls.String); v.Valid {
					stringSlice = append(stringSlice, fmt.Sprintf("'%s'", EscapeString(v.String)))
				} else {
					stringSlice = append(stringSlice, "NULL")
				}
			case NullsFloatType:
				if v := value.Interface().(nulls.Float64); v.Valid {
					if math.IsNaN(v.Float64) {
						stringSlice = append(stringSlice, "NULL")
					} else {
						stringSlice = append(stringSlice, fmt.Sprintf("%f", v.Float64))
					}
				} else {
					stringSlice = append(stringSlice, "NULL")
				}
			case NullsTimeType:
				if v := value.Interface().(nulls.Time); v.Valid {
					stringSlice = append(stringSlice, v.Time.Format("'2006-01-02 15:04:05'"))

				} else {
					stringSlice = append(stringSlice, "NULL")
				}
			case NullsBoolType:
				if v := value.Interface().(nulls.Bool); v.Valid {
					if v.Bool {
						stringSlice = append(stringSlice, "1")
					} else {
						stringSlice = append(stringSlice, "0")
					}
				} else {
					stringSlice = append(stringSlice, "NULL")
				}
			case SQLNullBoolType:
				if v := value.Interface().(sql.NullBool); v.Valid {
					if v.Bool {
						stringSlice = append(stringSlice, "1")
					} else {
						stringSlice = append(stringSlice, "0")
					}
				} else {
					stringSlice = append(stringSlice, "NULL")
				}
			case SQLNullInt64Type:
				v := value.Interface().(sql.NullInt64)
				if v.Valid {
					stringSlice = append(stringSlice, fmt.Sprintf("%d", v.Int64))
				} else {
					stringSlice = append(stringSlice, "NULL")
				}
			case SQLNullFloat64Type:
				if v := value.Interface().(sql.NullFloat64); v.Valid {
					if math.IsNaN(v.Float64) {
						stringSlice = append(stringSlice, "NULL")
					} else {
						stringSlice = append(stringSlice, fmt.Sprintf("%f", v.Float64))
					}
				} else {
					stringSlice = append(stringSlice, "NULL")
				}
			case SQLNullStringType:
				if v := value.Interface().(sql.NullString); v.Valid {
					stringSlice = append(stringSlice, fmt.Sprintf("'%s'", EscapeString(v.String)))
				} else {
					stringSlice = append(stringSlice, "NULL")
				}
			case OtherType:
				panic(fmt.Sprintf("unknown field type: %v", field.Type))
			}
		}
	}
	return fmt.Sprintf("(%s)", strings.Join(stringSlice, ","))
}

// CSVHeaders creates a slice of headers from a struct
func (c *Connection) CSVHeaders(v interface{}) []string {
	structValue := reflect.ValueOf(v)
	structType := structValue.Type()
	numFields := structValue.NumField()
	cols := make([]string, numFields)
	for i := 0; i < numFields; i++ {
		f := structType.Field(i)
		cols[i] = f.Tag.Get("db")
	}
	return cols
}

// ToSnakeCase conerts to snakecase
func ToSnakeCase(str string) string {
	var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

type table interface {
	TableName() string
}

// Scanner returns an slice of interface to a struct
// rows.Scan(seaspandb.Scanner(&m)...)
func Scanner(u interface{}) []interface{} {
	val := reflect.ValueOf(u).Elem()
	typ := val.Type()
	v := []interface{}{}
	for i := 0; i < val.NumField(); i++ {
		typeField := typ.Field(i)
		if typeField.Tag.Get("db") == "" {
			continue
		}
		valueField := val.Field(i)
		v = append(v, valueField.Addr().Interface())
	}
	return v
}

// ScanRow scans an interface pointer into a row
func ScanRow(rows *sql.Rows, v interface{}) error {
	t := reflect.TypeOf(v)
	if t.Kind() != reflect.Ptr {
		return errors.New("passed value to ScanRow must be a pointer")
	}
	if err := rows.Scan(Scanner(v)...); err != nil {
		return err
	}
	return nil
}

// CSVHeaders creates a slice of headers from a struct
func CSVHeaders(c interface{}) []string {
	structType := reflect.TypeOf(c)
	structValue := reflect.ValueOf(c)
	if structValue.Kind() == reflect.Ptr {
		structType = structType.Elem()
		structValue = structValue.Elem()
	}
	numFields := structValue.NumField()
	cols := make([]string, numFields)
	for i := 0; i < numFields; i++ {
		f := structType.Field(i)
		cols[i] = f.Tag.Get("db")
	}
	return cols
}

func Columns(c interface{}) []string {
	m := &Model{Value: c}
	return m.ColumnSlice()
}

func TableName(c interface{}) string {
	m := &Model{Value: c}
	return m.TableName()
}

// StructHeaders creates a slice of headers from a struct
func StructHeaders(v interface{}) []string {
	structValue := reflect.ValueOf(v)
	structType := structValue.Type()
	numFields := structValue.NumField()
	cols := make([]string, numFields)
	for i := 0; i < numFields; i++ {
		f := structType.Field(i)
		cols[i] = f.Name
	}
	return cols
}

// MustFormatMap formats a string from a map or panics
func MustFormatMap(s string, m map[string]string) string {
	if s, err := interpol.WithMap(s, m); err != nil {
		panic(err)
	} else {
		return s
	}
}

func createSchemaSlice(model *Model) string {
	v := reflect.Indirect(reflect.ValueOf(model.Value))
	columns := map[string]*Column{}
	order := map[int]string{}
	for i := 0; i < v.Len(); i++ {
		val := v.Index(i)
		var newModel *Model
		if val.Kind() == reflect.Ptr {
			newModel = &Model{Value: val.Interface()}
		} else {
			newModel = &Model{Value: val.Addr().Interface()}
		}
		cols := newModel.ToColumns()
		if len(columns) == 0 {
			for i, c := range cols {
				columns[c.Name] = c
				order[i] = c.Name
			}
			continue
		}
		for _, c := range cols {
			columns[c.Name].Update(c)
		}
	}
	out := make([]string, len(columns))
	for i, name := range order {
		out[i] = columns[name].String()
	}
	return strings.Join(out, "\n, ")
}

func createSchemaSingleton(model *Model) string {
	out := []string{}
	for _, c := range model.ToColumns() {
		out = append(out, c.String())
	}
	return strings.Join(out, "\n, ")

}

func CreateSchema(v interface{}) string {
	m := Model{Value: v}
	if m.isSlice() {
		return createSchemaSlice(&m)
	}
	return createSchemaSingleton(&m)
}
