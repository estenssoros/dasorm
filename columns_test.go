package dasorm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnToUpper(t *testing.T) {
	c := &Column{Name: "asdf"}
	assert.Equal(t, "ASDF", c.UpperName())
}

func TestColumnStringer(t *testing.T) {
	c := &Column{Name: "asdf", DataType: varcharColumn, Length: 10}
	assert.Equal(t, `"ASDF" VARCHAR(15)`, c.String())
	c.DataType = integerColumn
	assert.Equal(t, `"ASDF" INTEGER`, c.String())
}

func TestColumnUpdate(t *testing.T) {
	{
		column := &Column{Name: "asdf", DataType: varcharColumn, Length: 10}
		other := &Column{Name: "asdf", DataType: varcharColumn, Length: 15}
		column.Update(other)
		assert.Equal(t, other.Length, column.Length)
	}
	{
		column := &Column{Name: "asdf", DataType: integerColumn, Length: 10}
		other := &Column{Name: "asdf", DataType: integerColumn, Length: 15}
		old := column.Length
		column.Update(other)
		assert.Equal(t, old, column.Length)
	}

}
