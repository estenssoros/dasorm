package dasorm

import (
	"fmt"
	"testing"

	"gopkg.in/go-playground/assert.v1"
)

func TestConfigString(t *testing.T) {
	c := Config{
		Dialect:  "asdf",
		Database: "asdf",
		Host:     "asdf",
		Port:     "asdf",
		User:     "asdf",
		Password: "asdf",
		DSN:      "asdf",
	}
	s := `{"Dialect":"asdf","Database":"asdf","Host":"asdf","Port":"asdf","User":"asdf","Password":"asdf","DSN":"asdf"}`
	assert.Equal(t, s, c.String())
}

func TestConnectDialect(t *testing.T) {
	c := &Config{
		Dialect: "asdf",
	}
	_, err := ConnectDBConfig(c)
	assert.NotEqual(t, nil, err)
}

var driverErrWrapTests = []struct {
	in  string
	out string
}{
	{mysqlDialect, mysqlDriver},
	{postgresDialect, postgresDriver},
	{mssqlDialect, mssQLDriver},
	{snowflakeDialect, snowflakeDriver},
	{odbcDialect, odbcDriver},
}

func TestDriverErrWrap(t *testing.T) {
	for _, tt := range driverErrWrapTests {
		err := driverWrapErr(tt.in)
		outErr := fmt.Sprintf("add import statement: %s", tt.out)
		assert.Equal(t, outErr, err.Error())
	}
	err := driverWrapErr("asdf")
	assert.NotEqual(t, nil, err)
}
