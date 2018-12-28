package sqlite

import (
	"database/sql/driver"
	"encoding/json"
	"time"
)

type Time time.Time

func (t Time) String() string {
	return time.Time(t).Format("2006-01-02")
}

// NewTime returns a new, properly instantiated
// Time object.
func NewTime(t time.Time) Time {
	return Time(t)
}

func (t *Time) Scan(v interface{}) error {
	vt, err := time.Parse("2006-01-02 15:04:05-07:00", string(v.([]byte)))
	if err != nil {
		return err
	}
	*t = Time(vt)
	return nil
}

func (t Time) Value() (driver.Value, error) {
	return time.Time(t), nil
}

// MarshalJSON marshals the underlying value to a
// proper JSON representation.
func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t))
}

// UnmarshalJSON will unmarshal a JSON value into
// the propert representation of that value.
func (t *Time) UnmarshalJSON(text []byte) error {
	vt := time.Time{}
	if err := vt.UnmarshalJSON(text); err != nil {
		return err
	}
	*t = Time(vt)
	return nil
}

// UnmarshalText will unmarshal text value into
// the propert representation of that value.
func (t *Time) UnmarshalText(text []byte) error {
	return t.UnmarshalJSON(text)
}
