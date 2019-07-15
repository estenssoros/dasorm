package dasorm

import (
	"fmt"
	"strings"
)

var (
	varcharColumn  = "VARCHAR"
	integerColumn  = "INTEGER"
	floatColumn    = "FLOAT"
	booleanColumn  = "BOOLEAN"
	datetimeColumn = "DATETIME"
)

type Column struct {
	Name     string
	DataType string
	Length   int
}

func (c *Column) UpperName() string {
	return strings.ToUpper(c.Name)
}

func (c Column) String() string {
	switch c.DataType {
	case varcharColumn:
		return fmt.Sprintf(`"%s" %s(%d)`, c.UpperName(), varcharColumn, int(float64(c.Length)*1.5))
	}
	return fmt.Sprintf(`"%s" %s`, c.UpperName(), c.DataType)
}

func (c *Column) Update(other *Column) {
	if c.DataType != "VARCHAR" {
		return
	}
	if other.Length > c.Length {
		c.Length = other.Length
	}
}
