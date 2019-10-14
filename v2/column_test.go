package dasorm

import (
	"testing"

	"gopkg.in/go-playground/assert.v1"
)

func TestColumnUpperName(t *testing.T) {
	c := &Column{
		Name: "asdf",
	}
	assert.Equal(t, "ASDF", c.UpperName())
}

func TestColumnString(t *testing.T) {
	{
		c := &Column{
			Name:     "asdf",
			DataType: varcharColumn,
			Length:   10,
		}
		assert.Equal(t, `"ASDF" VARCHAR(15)`, c.String())
	}
	{
		c := &Column{
			Name:     "asdf",
			DataType: integerColumn,
			Length:   10,
		}
		assert.Equal(t, `"ASDF" INTEGER`, c.String())
	}
}

func TestColumnUpdate(t *testing.T) {
	c1 := &Column{
		Name:     "asdf",
		DataType: varcharColumn,
		Length:   10,
	}
	c2 := &Column{
		Name:     "asdf",
		DataType: integerColumn,
		Length:   20,
	}
	c1.Update(c2)
	assert.Equal(t, 20, c1.Length)
	c := &Column{
		Name: "asdf",
	}
	c.Update(c2)
	assert.Equal(t, 0, c.Length)
}
