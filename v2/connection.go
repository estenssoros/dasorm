package dasorm

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// DB waraps sqlx.DB
type DB struct {
	*sqlx.DB
	debug bool
}

// Connection holds a pointer to the database connection
type Connection struct {
	*DB
	Dialect dialect
}

// Close wraps db.close
func (c *Connection) Close() {
	c.DB.Close()
}

// Debug sets the db to debug
func (c *Connection) Debug(d bool) {
	c.debug = d
}

// connectDBHandler reads creds from service and provides databse connection
func connectDBHandler(server string) (*Connection, error) {
	config, err := GetConfigVault(server)
	if err != nil {
		return nil, errors.Wrap(err, server)
	}
	return ConnectDBConfig(config)
}

// Ping wraps the db ping method
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
		conn, err := connectDBHandler(server)
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

// ConnectDBTimeout attempts to connect with a custom timeout
func ConnectDBTimeout(server string, timeout int) (*Connection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	ch := make(chan struct {
		conn *Connection
		err  error
	})

	go func() {
		conn, err := connectDBHandler(server)
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

// Query wraps the query method
func (c *Connection) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return c.DB.Query(query, args...)
}

// QueryContext wraps the QueryContext method
func (c *Connection) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return c.DB.QueryContext(ctx, query, args...)
}

// QueryRowContext wraps the QueryRowContext method
func (c *Connection) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.DB.QueryRowContext(ctx, query, args...)
}

// QueryRow wraps the QueryRowContext method
func (c *Connection) QueryRow(query string, args ...interface{}) *sql.Row {
	return c.DB.QueryRow(query, args...)
}

// ExecContext wraps the ExecContext method
func (c *Connection) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return c.DB.ExecContext(ctx, query, args...)
}

// Exec wraps the ExecContext method
func (c *Connection) Exec(query string, args ...interface{}) (sql.Result, error) {
	return c.DB.Exec(query, args...)
}

// WriteTuples writes tuples to database
func (c *Connection) WriteTuples(insertStmt string, tuples []string) error {
	if _, err := c.DB.Exec(insertStmt + strings.Join(tuples, ",")); err != nil {
		for _, t := range tuples {
			if _, err := c.DB.Exec(insertStmt + t); err != nil {
				return errors.Wrap(err, insertStmt+t)
			}
		}
	}
	return nil
}

// DialectName return the dialect name
func (c *Connection) DialectName() string {
	return c.Dialect.Name()
}

func connectURL(driver, connectionURL string) (*sqlx.DB, error) {
	db, err := sqlx.Connect(driver, connectionURL)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}
