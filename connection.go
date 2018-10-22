package dasorm

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// Connection holds a pointer to the database connection
type Connection struct {
	DB      *sqlx.DB
	Dialect dialect
}

// Close wraps db.close
func (c *Connection) Close() {
	c.DB.Close()
}

// Config holds database information
type Config struct {
	Dialect  string `vault:"dialect"`
	Database string `vault:"database"`
	Host     string `vault:"host"`
	Port     string `vault:"port"`
	User     string `vault:"user"`
	Password string `vault:"password"`
}

// ConnectDB reads creds from service and provides databse connection
func connectDB(server string) (*Connection, error) {
	creds, err := getConfigVault(server)
	if err != nil {
		return nil, errors.Wrap(err, server)
	}
	switch creds.Dialect {
	case "mysql":
		return connectMySQL(creds)
	default:
		return nil, fmt.Errorf("%s dialect not recognized", creds.Dialect)
	}
}

func (c *Connection) Ping() error {
	return c.DB.Ping()
}

// ConnectDB connects to a database environment
func ConnectDB(server string) (*Connection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch := make(chan struct {
		conn *Connection
		err  error
	})

	go func() {
		conn, err := connectDB(server)
		ch <- struct {
			conn *Connection
			err  error
		}{conn, err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case pack := <-ch:
		if pack.err != nil {
			return nil, pack.err
		}
		return pack.conn, nil
	}
}
func (c *Connection) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return c.DB.Query(query, args...)
}

func (c *Connection) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return c.DB.QueryContext(ctx, query, args...)
}

func (c *Connection) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.DB.QueryRowContext(ctx, query, args...)
}

func (c *Connection) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return c.DB.ExecContext(ctx, query, args...)
}
