# dasorm
Golang orm thing that borrows the best parts of `github.com/gobuffalo/pop` and adds some other things I found necessary and helpful. 

Currently only supports MySQL

## Vault for credential management
dasorm relies on database credentials stored in the vault kv system.

User must have set the environment variable `VAULT_ADDR` *and* either have set a `VAULT_TOKEN` environment variable or have a valid vault token stored in `$HOME/.vault_token`.

vault kv convention:

```
secret/data/<your_environment>/database
```


data format:

```
====== Data ======
Key         Value
---         -----
database    <database_name>
dialect     mysql
host        <host>
password    <password>
port        3306
user        <user_name>
```