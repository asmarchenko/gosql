gosql
=====

Utility tools for working with sql databases

## Instalation
  $ go get github.com/asmarchenko/gosql


## MySQL Databases:
- autoclosable connections
- supporting transactions
- connections pool
- mysql handler
- custom ResultSet struct for java developers :)

## MySQL examples:

Create connection:
  `
  db, err = gosql.NewMySQLConnection("localhost", 3306, "username", "password", "dbname")
  `

Create connections pool:
  `
  config := gosql.NewMySQLConfig("localhost", 3306, "username", "password", "dbname")
  poolSize := 100
	pool, err := gosql.NewMySQLConnectionPool(poolSize, config)
  defer pool.CloseAll()
  
  connection := pool.Get()
  ......
  pool.Release(connection)
  `
  
Working with MySQL HANDLER:
  `
  handler := dBase.CreateMySQLHandler("table_name")
	defer handler.Close()
	
	for handler.HasNext() {
		result := handler.Next("id < 100", "index-field", 1000)
		for result.Next() {
			......
		}
	}
   `

## SQLite3 Databases:
Create connection:
  `
  db, err := gosql.NewSQLiteConnection("/tmp/sqlite.db")
  `
