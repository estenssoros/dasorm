package dasorm

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
)

type sqlBuilder struct {
	Query Query
	Model *Model
	sql   string
	args  []interface{}
}

func newSQLBuilder(q Query, m *Model) *sqlBuilder {
	return &sqlBuilder{
		Query: q,
		Model: m,
		args:  []interface{}{},
	}
}

func (sq *sqlBuilder) String() string {
	if sq.sql == "" {
		sq.compile()
	}
	return sq.sql
}

func (sq *sqlBuilder) Args() []interface{} {
	if len(sq.args) == 0 {
		if len(sq.Query.RawSQL.Arguments) > 0 {
			sq.args = sq.Query.RawSQL.Arguments
		} else {
			sq.compile()
		}
	}
	return sq.args
}

var inRegex = regexp.MustCompile(`(?i)in\s*\(\s*\?\s*\)`)

func (sq *sqlBuilder) compile() {
	if sq.sql == "" {
		if sq.Query.RawSQL.Fragment != "" {
			sq.sql = sq.Query.RawSQL.Fragment
		} else {
			sq.sql = sq.buildSelectSQL()
		}

		if inRegex.MatchString(sq.sql) {
			s, _, err := sqlx.In(sq.sql, sq.Args())
			if err == nil {
				sq.sql = s
			}
		}
		sq.sql = sq.Query.Connection.Dialect.TranslateSQL(sq.sql)
	}
}

func (sq *sqlBuilder) buildSelectSQL() string {
	cols := sq.buildColumns()
	var sql string
	switch sq.Query.Connection.DialectName() {
	case "mssql":
		sql = "SELECT "
		sql = sq.buildPaginationClauses(sql)
		sql += fmt.Sprintf("%s FROM %s", strings.Join(cols, ","), sq.Model.TableName())
		sql = sq.buildWhereClauses(sql)
		sql = sq.buildOrderClauses(sql)
	default:
		sql = fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ","), sq.Model.TableName())
		sql = sq.buildWhereClauses(sql)
		sql = sq.buildOrderClauses(sql)
		sql = sq.buildPaginationClauses(sql)
	}
	return sql
}

func (sq *sqlBuilder) buildWhereClauses(sql string) string {
	wc := sq.Query.whereClauses
	if len(wc) > 0 {
		sql = fmt.Sprintf("%s WHERE %s", sql, wc.Join(" AND "))
		sq.args = append(sq.args, wc.Args()...)
	}
	return sql
}

func (sq *sqlBuilder) buildOrderClauses(sql string) string {
	oc := sq.Query.orderClauses
	if len(oc) > 0 {
		sql = fmt.Sprintf("%s ORDER BY %s", sql, oc.Join(", "))
		sq.args = append(sq.args, oc.Args()...)
	}
	return sql
}

func (sq *sqlBuilder) buildPaginationClauses(sql string) string {
	if sq.Query.limitResults > 0 {
		switch sq.Query.Connection.DialectName() {
		case "mssql":
			sql += fmt.Sprintf("TOP %d ", sq.Query.limitResults)
		default:
			sql = fmt.Sprintf("%s LIMIT %d", sql, sq.Query.limitResults)
		}
	}
	return sql
}

var columnCache = map[string][]string{}
var columnCacheMutex = sync.Mutex{}

// buildColumns either caches or creates new columns for a table
func (sq *sqlBuilder) buildColumns() []string {
	tableName := sq.Model.TableName()

	columnCacheMutex.Lock()
	cols, ok := columnCache[tableName]
	columnCacheMutex.Unlock()

	if ok {
		return cols
	}

	cols = sq.Model.ColumnSlice()
	columnCacheMutex.Lock()
	columnCache[tableName] = cols
	columnCacheMutex.Unlock()

	return cols
}
