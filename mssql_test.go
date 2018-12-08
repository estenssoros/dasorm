package dasorm

import "testing"

func TestMSSQL(t *testing.T) {
	db, err := ConnectDB("jde")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
}
