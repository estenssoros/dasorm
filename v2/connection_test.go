package dasorm

import (
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
func TestConnectDBHandler(t *testing.T) {
	_, err := connectDBHandler("asdf")
	assert.NotEqual(t, nil, err)
	conn, err := ConnectDB("dev-local")
	assert.Equal(t, nil, err)
	err = conn.Close()
	assert.Equal(t, nil, err)
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
}
