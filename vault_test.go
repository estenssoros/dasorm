package dasorm

import (
	"testing"
)

func TestVaultGetEnv(t *testing.T) {
	if _, err := getEnv("HOME"); err != nil {
		t.Error(err)
	}
	_, err := getEnv("asdasdffdas")
	if err == nil {
		t.Error("should error")
	}
}

func TestGetValutToken(t *testing.T) {
	if _, err := getVaultToken(); err != nil {
		t.Error(err)
	}
}

func TestConnectVault(t *testing.T) {
	if _, err := connectVault(); err != nil {
		t.Error(err)
	}
}

func TestGetConfigVault(t *testing.T) {
	if _, err := getConfigVault("dev-local"); err != nil {
		t.Error(err)
	}
	if _, err := GetConfigVault("dev-local"); err != nil {
		t.Error(err)
	}
}

func TestGetAWSCreds(t *testing.T) {
	if _, err := GetAWSCreds("firehose"); err != nil {
		t.Error(err)
	}
}
