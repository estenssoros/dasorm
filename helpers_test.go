package dasorm

import (
	"fmt"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
)

type TestStruct struct {
	ID        uuid.UUID
	Name      string
	CreatedAt time.Time
	AnInt     int
	AFloat    float64
	ABool     bool
}

func TestMapToStruct(t *testing.T) {
	m := map[string]interface{}{
		"ID":        uuid.Must(uuid.NewV4()),
		"Name":      "asdf",
		"CreatedAt": time.Now(),
		"AnInt":     7,
		"AFloat":    7.0,
		"ABool":     true,
	}
	v := &TestStruct{}
	if err := MapToStruct(v, m); err != nil {
		t.Fatal(err)
	}
	fmt.Println(v)
}
