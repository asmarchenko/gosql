package tests

import (
	"gosql"
	"fmt"
)

func TestSQLite() {

	con, err := gosql.NewSQLiteConnection("/tmp/hm.db")
	if err != nil {
		fmt.Println(err)
	} else {
		_, err := con.ExecuteUpdate("CREATE TABLE `user` (`uname`  TEXT NOT NULL, PRIMARY KEY (`uname`));")
		fmt.Println(err)

		id, err := con.ExecuteInsert("INSERT INTO `user` VALUES (?)", "secondUser")
		fmt.Println(id)

		rows, err := con.ExecuteUpdate("UPDATE `user` SET `uname`=? WHERE `uname`=?;", "second222User", "secondUser")
		fmt.Println(rows)

		rs, _ := con.ExecuteQuery("SELECT * FROM `user`;")
		for rs.Next() {
			fmt.Println(rs.GetString(1))
		}
	}
}