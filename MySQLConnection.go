package gosql

import (
	"fmt"
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"github.com/xlab/closer"
	"strconv"
	"gosql/utils"
	"time"
	"sync"
)

//////////////////// MySQLConfig ////////////////////////
type MySQLConfig struct {
	Host   string
	Port   int
	User   string
	Pass   string
	DbName string
}

func NewMySQLConfig(host string, port int, user string, password string, dataBase string) *MySQLConfig {
	return &MySQLConfig{host, port, user, password, dataBase}
}


func (conf *MySQLConfig) String() string {
	return conf.User + "@" + conf.Host + ":" + strconv.Itoa(conf.Port) + "/" + conf.DbName
}


/////////////////// MySQLConnection //////////////////////
type MySQLConnection struct {
	hash uint32
	Config   *MySQLConfig
	dataBase *sql.DB
	isTransaction bool
	transaction *sql.Tx
}

func NewMySQLConnection(host string, port int, user string, password string, dataBase string) (MySQLConnection, error) {
	conf := NewMySQLConfig(host, port, user, password, dataBase)
	return CreateMySQLConnection(conf)
}

func CreateMySQLConnection(config *MySQLConfig) (MySQLConnection, error) {

	db := MySQLConnection{Config: config, hash:utils.GenerateHash(), isTransaction:false}
	err := db.connect()
	if err != nil {
		fmt.Println(err)
		return db, err
	}

	closer.Bind(db.Close)
	return db, nil
}

func (db *MySQLConnection) checkConnection() {
	err := db.dataBase.Ping()
	if err != nil {
		fmt.Println("DB reconnect")
		db.connect()
	}
}

func (db *MySQLConnection) connect() error {
	dataBase, err := sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v",
		db.Config.User, db.Config.Pass, db.Config.Host, db.Config.Port, db.Config.DbName))
	if err != nil {
		return err
	}

	err = dataBase.Ping()
	if err != nil {
		return err
	}

	db.dataBase = dataBase
	return nil
}

func (db *MySQLConnection) Hash() uint32 {
	return db.hash
}

func (db *MySQLConnection) Close() {
	db.dataBase.Close()
}

func _()  {
	fmt.Println(mysql.ErrBusyBuffer)
}

func (db *MySQLConnection) String() string {
	return "[id=" + fmt.Sprint(db.hash) + "] [" + db.Config.String() + "]"
}


///////////////// MySQLConnectionPool ////////////////////
type MySQLConnectionPool struct {
	sync.Mutex
	blocked map[uint32]bool
	connections []MySQLConnection
}

func NewMySQLConnectionPool(size int, config *MySQLConfig) (*MySQLConnectionPool, error) {

	pool := &MySQLConnectionPool{connections:make([]MySQLConnection, size), blocked:make(map[uint32]bool)}
	for i := 0; i < size; i++ {
		connection, err := CreateMySQLConnection(config)
		if err == nil {
			pool.connections[i] = connection
			pool.blocked[connection.hash] = false
		} else {
			return nil, err
		}
	}

	fmt.Println("Pool created")
	return pool, nil
}

func (pool *MySQLConnectionPool) Get() *MySQLConnection {

	pool.Lock()
	defer pool.Unlock()

	for _, con := range pool.connections {
		if !pool.blocked[con.hash] {
			pool.blocked[con.hash] = true
			con.checkConnection()
			return &con
		}
	}

	time.Sleep(500 * time.Millisecond)
	return pool.Get()
}

func (pool *MySQLConnectionPool) Release(connection *MySQLConnection) {

	pool.Lock()
	defer pool.Unlock()

	pool.blocked[connection.hash] = false
}

func (pool *MySQLConnectionPool) Size() int {

	pool.Lock()
	defer pool.Unlock()

	size := 0
	for _, blocked := range pool.blocked {
		if !blocked {
			size++
		}
	}
	return size
}

func (pool *MySQLConnectionPool) CloseAll() {
	for _, c := range pool.connections {
		pool.blocked[c.hash] = true
		c.Close()
	}
	fmt.Println("Connections are closed")
}

func (pool *MySQLConnectionPool) String() string {
	poolInfo := "Pool: \n"
	for _, c := range pool.connections {
		conInfo := "[blocked=" + strconv.FormatBool(pool.blocked[c.hash]) + "] " + c.String() + "\n"
		poolInfo += conInfo
	}
	return poolInfo
}