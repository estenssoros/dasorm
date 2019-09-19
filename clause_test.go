package dasorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJoinClause(t *testing.T) {
	cs := []clause{
		{Fragment: "asdf", Arguments: []interface{}{"asdf", "asdf"}},
		{Fragment: "asdf", Arguments: []interface{}{"asdf", "asdf"}},
	}
	have := clauses(cs).Join(",")
	assert.Equal(t, "asdf,asdf", have)
	assert.Equal(t, 4, len(clauses(cs).Args()))
}
