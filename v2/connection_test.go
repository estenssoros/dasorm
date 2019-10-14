package dasorm

import (
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"gopkg.in/go-playground/assert.v1"
)

func TestConnectionDebug(t *testing.T) {
	c := &Connection{
		DB: &DB{},
	}
	c.SetDebug(true)
	assert.Equal(t, true, c.DB.Debug())
}

var dialectNameTests = []struct {
	dialect dialect
	name    string
}{
	{&mysql{}, "mysql"},
	{&mssql{}, "mssql"},
	{&odbc{}, "odbc"},
	{&postgres{}, "postgres"},
	{&snowflake{}, "snowflake"},
}

func TestDialectName(t *testing.T) {
	for _, tt := range dialectNameTests {
		c := &Connection{Dialect: tt.dialect}
		assert.Equal(t, c.DialectName(), tt.name)

	}
}

func TestUknownDriver(t *testing.T) {
	_, err := sqlx.Open("asdf", "")
	if !isErrUknownDriver(err) {
		t.Error("should be uknown driver")
	}
}

func TestConnectURL(t *testing.T) {
	_, err := connectURL("asdf", "asdf")
	assert.NotEqual(t, nil, err)
	assert.Equal(t, true, strings.Contains(err.Error(), "unknown driver"))
}

var unknownDriverConnectDialectTest = []struct {
	dialect string
}{
	{mysqlDialect},
	{postgresDialect},
	{mssqlDialect},
	{snowflakeDialect},
	{odbcDialect},
}

func TestUnknownDriverConnectDialect(t *testing.T) {
	for _, tt := range unknownDriverConnectDialectTest {
		_, err := connectURL(tt.dialect, "")
		assert.NotEqual(t, nil, err)
		assert.Equal(t, true, strings.Contains(err.Error(), "add import statement"))
	}
}
