package dasorm

import (
	"testing"
)

func TestGetODBConfig(t *testing.T) {
	_, err := getConfigVault("netsuite")
	if err != nil {
		t.Error(err)
	}
}
