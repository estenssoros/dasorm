package sqlite

import (
	"database/sql/driver"
	"encoding/json"
	"time"
)

type Date time.Time

func (t Date) String() string {
	return time.Time(t).Format("01/02/2006")
}

// NewTime returns a new, properly instantiated
// Time object.
func NewDate(d time.Time) Date {
	return Date(d)
}

func (d *Date) Scan(v interface{}) error {
	vt, err := time.Parse("2006-01-02 15:04:05-07:00", string(v.([]byte)))
	if err != nil {
		return err
	}
	*d = Date(vt)
	return nil
}

func (d Date) Value() (driver.Value, error) {
	return time.Time(d), nil
}

// MarshalJSON marshals the underlying value to a
// proper JSON representation.
func (d Date) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(d))
}

// UnmarshalJSON will unmarshal a JSON value into
// the propert representation of that value.
func (d *Date) UnmarshalJSON(text []byte) error {
	vt := time.Time{}
	if err := vt.UnmarshalJSON(text); err != nil {
		return err
	}
	*d = Date(vt)
	return nil
}

// UnmarshalText will unmarshal text value into
// the propert representation of that value.
func (d *Date) UnmarshalText(text []byte) error {
	return d.UnmarshalJSON(text)
}
