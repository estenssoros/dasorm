package dasorm

import (
	"encoding/json"

	"github.com/pkg/errors"
)

// Config holds database information
type Config struct {
	Dialect  string `vault:"dialect"`
	Database string `vault:"database"`
	Host     string `vault:"host"`
	Port     string `vault:"port"`
	User     string `vault:"user"`
	Password string `vault:"password"`
	DSN      string `vault:"dsn"`
}

func (c Config) String() string {
	ju, _ := json.Marshal(c)
	return string(ju)
}

var (
	mysqlDialect     = "mysql"
	postgresDialect  = "postgres"
	mssqlDialect     = "microsoft_sql"
	snowflakeDialect = "snowflake"
	odbcDialect      = "odbc"
)

// ConnectDBConfig connects to db given config
func ConnectDBConfig(config *Config) (*Connection, error) {
	switch config.Dialect {
	case mysqlDialect:
		return connectMySQL(config)
	case postgresDialect:
		return connectPostgres(config)
	case mssqlDialect:
		return connectMSSQL(config)
	case snowflakeDialect:
		return connectSnowflake(config)
	case odbcDialect:
		return connectODBC(config)
	default:
		return nil, errors.Errorf("%s dialect not recognized", config.Dialect)
	}
}
