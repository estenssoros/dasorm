package dasorm

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/hashicorp/vault/api"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/pkg/errors"
)

// check environment for a variable
func getEnv(key string) (string, error) {
	val := os.Getenv(key)
	if val == "" {
		return val, fmt.Errorf("missing environment variable: %s", key)
	}
	return val, nil
}

// vault will store a local token. check for that first
// if the files does not exist, check environment variables
func getVaultToken() (string, error) {
	homeDir, err := homedir.Dir()
	if err != nil {
		return "", err
	}
	file, err := ioutil.ReadFile(path.Join(homeDir, ".vault-token"))
	if err != nil {
		token, err := getEnv("VAULT_TOKEN")
		if err != nil {
			return "", err
		}
		return token, nil
	}
	return string(file), nil
}

// connect to vault
func connectVault() (*api.Client, error) {
	token, err := getVaultToken()
	if err != nil {
		return nil, err
	}
	vaultAddr, err := getEnv("VAULT_ADDR")
	if err != nil {
		return nil, err
	}

	client, err := api.NewClient(&api.Config{
		Address: vaultAddr,
	})
	if err != nil {
		return nil, err
	}
	client.SetToken(token)
	return client, nil
}

// use vault api and reflect to populate config struct
func getConfigVault(environment string) (*Config, error) {
	client, err := connectVault()
	if err != nil {
		return nil, err
	}
	secret, err := client.Logical().Read(fmt.Sprintf("secret/data/%s/database", environment))
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, fmt.Errorf("vault error: no data at: %s", environment)
	}

	dataMap, ok := secret.Data["data"].(map[string]interface{})
	if !ok {
		return nil, errors.New("failed to parse data from vault response")
	}

	config := &Config{}
	configVals := reflect.ValueOf(config).Elem()
	configType := configVals.Type()

	for i := 0; i < configVals.NumField(); i++ {
		f := configType.Field(i)
		tagName := f.Tag.Get("vault")
		fmt.Println(tagName)
		if tagName == "" {
			return nil, errors.Errorf("unknown field in vault config: %s", f.Name)
		}
		val, ok := dataMap[tagName]
		if !ok {
			continue
			// return nil, fmt.Errorf("could not locate %s in vault response", f.Name)
		}
		configVals.Field(i).SetString(val.(string))
	}
	fmt.Println(config)
	return config, nil
}

// GetConfigVault uses a context to attempt to connect to vault
func GetConfigVault(environment string) (*Config, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch := make(chan struct {
		conn *Config
		err  error
	})

	go func() {
		conn, err := getConfigVault(environment)
		ch <- struct {
			conn *Config
			err  error
		}{conn, err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case pack := <-ch:
		if pack.err != nil {
			return nil, pack.err
		}
		return pack.conn, nil
	}

}

//AWSCreds stores the creds for an aws user
type AWSCreds struct {
	ID  string
	Key string
}

//GetAWSCreds returns the creds for logging
func GetAWSCreds(service string) (*AWSCreds, error) {
	client, err := connectVault()
	if err != nil {
		return nil, errors.Wrap(err, "connect vault")
	}

	secrets, err := client.Logical().Read(fmt.Sprintf("secret/data/%s", service))
	if err != nil {
		return nil, errors.Wrap(err, "client read")
	}

	if secrets == nil {
		return nil, fmt.Errorf("vault errror: no data at: %s", service)
	}

	secret := secrets.Data["data"].(map[string]interface{})

	creds := &AWSCreds{}
	if val, ok := secret["aws_id"]; !ok {
		panic(errors.New("Could not locate aws credentials"))
	} else {
		creds.ID = val.(string)
	}
	if val, ok := secret["aws_key"]; !ok {
		panic(errors.New("Could not locate aws credentials"))
	} else {
		creds.Key = val.(string)
	}

	return creds, nil
}
