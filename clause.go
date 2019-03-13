package dasorm

import (
	"strings"
)

type clause struct {
	Fragment  string
	Arguments []interface{}
}

type clauses []clause

func (c clauses) Join(sep string) string {
	out := make([]string, 0, len(c))
	for _, clause := range c {
		out = append(out, clause.Fragment)
	}
	return strings.Join(out, sep)
}

func (c clauses) Args() (args []interface{}) {
	for _, clause := range c {
		args = append(args, clause.Arguments...)
	}
	return
}
