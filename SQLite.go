package gosql

import (
	"github.com/mattn/go-sqlite3"
	"database/sql"
	"fmt"
)

type SQLiteConnection struct {
	path string
	db *sql.DB
}

func NewSQLiteConnection(path string) (*SQLiteConnection, error) {

	dBase, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	return &SQLiteConnection{path: path, db: dBase}, nil
}

// returns ResutSet struct like an java object
func (con *SQLiteConnection) ExecuteQuery(query string, params ... interface{}) (ResultSet, error) {

	var result ResultSet
	paramPointers := make([]interface{}, len(params))
	for i := range params {
		paramPointers[i] = &params[i]
	}

	rows, err := con.db.Query(query, paramPointers...)
	defer func() {
		if err == nil {rows.Close()}
	}()

	if err == nil {

		columnTypes, _ := rows.ColumnTypes()
		result.prepareMetaData(columnTypes)

		columns, _ := rows.Columns()
		columnsCnt := len(columns)

		for rows.Next() {
			val := make([][]byte, columnsCnt)
			valPointers := make([]interface{}, columnsCnt)

			for i := range val {
				valPointers[i] = &val[i]
			}

			if err := rows.Scan(valPointers...); err != nil {
				return result, err
			}

			row := NewRow()
			for i, columnName := range columns {
				value := valPointers[i].(*[]byte)
				row.addToRow(columnName, *value)
			}
			result.addRow(row)
		}
	}
	return result, err
}

// returns last insert id or error
func (con *SQLiteConnection) ExecuteInsert(query string, params ... interface{}) (int64, error) {

	paramPointers := make([]interface{}, len(params))
	for i := range params {
		paramPointers[i] = &params[i]
	}

	res, err := con.db.Exec(query, paramPointers...)
	if err == nil {
		return res.LastInsertId()
	}
	return 0, err
}

// returns affected rows count or error
func (con *SQLiteConnection) ExecuteUpdate(query string, params ... interface{}) (int64, error) {

	paramPointers := make([]interface{}, len(params))
	for i := range params {
		paramPointers[i] = &params[i]
	}

	res, err := con.db.Exec(query, paramPointers...)
	if err == nil {
		return res.RowsAffected()
	}
	return 0, err
}

func (con *SQLiteConnection) Close() {
	con.db.Close()
}

func _() {
	fmt.Println(sqlite3.ErrFormat)
}