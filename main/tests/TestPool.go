package tests

import (
	"gosql"
	"fmt"
	"time"
)

func TestPool() {

	conf := gosql.NewMySQLConfig("localhost", 3306, "alex", "111", "testDB")
	pool, err := gosql.NewMySQLConnectionPool(100, conf)

	time.Sleep(10 * time.Second)
	panic("aaa")

	defer pool.CloseAll()

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(pool)
		fmt.Println("size:", pool.Size())

		con1 := pool.Get()
		con2 := pool.Get()
		con3 := pool.Get()
		con4 := pool.Get()
		con5 := pool.Get()

		fmt.Println(pool)
		fmt.Println("size:", pool.Size())

		pool.Release(con1)
		pool.Release(con2)
		pool.Release(con3)

		fmt.Println(pool)
		fmt.Println("size:", pool.Size())

		pool.Release(con4)
		pool.Release(con5)

		fmt.Println(pool)
		fmt.Println("size:", pool.Size())
	}
}