package dasorm

import (
	"fmt"
	"testing"
)

func TestConnectPostgres(t *testing.T) {
	db, err := ConnectDB("vrs")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
}

type DailyVesselReport struct {
	ID       int `db:"id"`
	RemoteID int `db:"remote_id"`
}

func (dvr DailyVesselReport) TableName() string {
	return `daily_vessel_reports`
}

func TestQueryPostGres(t *testing.T) {
	dvr := &DailyVesselReport{}
	db, err := ConnectDB("vrs")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.First(dvr); err != nil {
		t.Fatal(err)
	}
	fmt.Println(dvr)
}
