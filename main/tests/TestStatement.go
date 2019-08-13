package tests

import (
	"fmt"
	"gosql"
	"time"
)

var dBase gosql.MySQLConnection

func TestStatement() {

	var err error
	dBase, err = gosql.NewMySQLConnection("localhost", 3306, "alex", "111", "testDB")

	if err != nil {
		fmt.Println(err)
	} else {
		//testHandler()
		//testCall()
		//testSelect()
		testInsert()
		//testReplace()
		//testUpdate()
	}

	dBase.Close()
}

func testHandler() {
	handler := dBase.CreateMySQLHandler("test_table")
	defer handler.Close()

	for handler.HasNext() {
		rs := handler.Next("id<100", "index-field", 30)
		for rs.Next() {
			rs.Print()
		}
		time.Sleep(1 * time.Second)
	}
}


func testCall() {
	rs, err := dBase.ExecuteCall("testCall", 23)
	if err != nil {
		fmt.Println(err)
	} else {
		for rs.Next()  {
			rs.Print()
		}
	}
}

func testSelect() {
	rs, err := dBase.Select("SELECT * FROM test_table2 LIMIT 1")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(rs.GetMetaData().Hash())
	}
}

func testInsert() {
	ins := dBase.Insert("test_table2").Fields("one", "two", "three")
	ins.Value(1, "string1", []byte("string11"))
	ins.Value(2, "string2", []byte("string22"))
	ins.Value(3, "string;0", []byte("string33"))
	fmt.Println(ins.String())
	res, err := ins.Run()
	fmt.Println(res)
	fmt.Println(err.String())
}

func testReplace() {
	rep := dBase.Replace("test_table").Fields("field").Value("string1")
	fmt.Println(rep.String())
	rep.Run()
}

func testUpdate() {
	upd := dBase.Update("test_table2")
	upd.SetField("two", "string002").SetField("three", []byte("kuba022")).WhereField("one", 2).Limit(1)
	fmt.Println(upd.String())
	upd.Run()
}