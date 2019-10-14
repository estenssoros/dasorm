package dasorm_test

import (
	"testing"

	"github.com/estenssoros/dasorm/v2"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/go-playground/assert.v1"
)

func TestConnectMysql(t *testing.T) {
	db, err := dasorm.ConnectDB("dev-local")
	assert.Equal(t, nil, err)
	defer db.Close()
	assert.Equal(t, nil, db.Ping())
	assert.Equal(t, "mysql", db.DialectName())
}
func TestConnectMysqlTimeout(t *testing.T) {
	db, err := dasorm.ConnectDBTimeout("dev-local", 5)
	assert.Equal(t, nil, err)
	defer db.Close()
	assert.Equal(t, nil, db.Ping())
}
