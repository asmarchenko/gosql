package tests

import (
	"fmt"
	"gosql"
)

func TestDump() {

	dBase, err := gosql.NewMySQLConnection("localhost", 3306, "alex", "111", "exite")

	if err != nil {
		fmt.Println(err)
	} else {
		//dmp, err := dBase.DumpTable("docs", "intDocID=0", 10)
		dmp, err := dBase.DumpByQuery("SELECT intFileID FROM exite_ftp_files WHERE intUserID=64428 LIMIT 10", "exite_ftp_files", "exite_ftp_files_new", "exite_ftp_files_body")
		if err != nil {
			fmt.Println(err)
		} else {
			for _, line := range dmp {
				fmt.Println(line)
			}
		}
	}
}