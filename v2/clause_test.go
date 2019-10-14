package dasorm

import (
	"testing"

	"gopkg.in/go-playground/assert.v1"
)

func TestClauseJoin(t *testing.T) {
	cs := clauses([]clause{
		{Fragment: "asdf", Arguments: []interface{}{"asdf"}},
		{Fragment: "asdf"},
	})

	assert.Equal(t, "asdf,asdf", cs.Join(","))
	assert.Equal(t, []interface{}{"asdf"}, cs.Args())
}
