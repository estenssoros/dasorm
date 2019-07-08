package dasorm

import "fmt"

// InsertStmt creates insert statement from struct tags
func InsertStmt(t interface{}) string {
	m := &Model{Value: t}
	stmt := "INSERT INTO %s (%s) VALUES"
	return fmt.Sprintf(stmt, m.TableName(), m.Columns())
}

// ReplaceStmt creates insert replac estatement from struct tags
func ReplaceStmt(t interface{}) string {
	m := &Model{Value: t}
	stmt := "REPLACE INTO %s (%s) VALUES"
	return fmt.Sprintf(stmt, m.TableName(), m.Columns())
}

// SelectStmt generates a select statement from a struct
func SelectStmt(t interface{}) string {
	m := &Model{Value: t}
	stmt := "SELECT %s FROM %s"
	return fmt.Sprintf(stmt, m.Columns(), m.TableName())
}

// TruncateStmt return the truncate statement for a table
func TruncateStmt(t interface{}) string {
	m := &Model{Value: t}
	return fmt.Sprintf("TRUNCATE TABLE %s", m.TableName())
}

//InsertIgnoreStmt executes insert ignore
func InsertIgnoreStmt(t interface{}) string {
	m := &Model{Value: t}
	stmt := `INSERT IGNORE INTO %s (%s) VALUES`
	return fmt.Sprintf(stmt, m.TableName(), m.Columns())
}
