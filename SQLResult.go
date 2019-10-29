package gosql

import (
	"strconv"
	"fmt"
	"strings"
	"hash/fnv"
	"database/sql"
	"github.com/go-sql-driver/mysql"
)

type Cell struct {
	Index int
	Name string
	Value []byte
}

type Row struct {
	Values []Cell
}

type ResultSetMetaData struct {
	columnCount int
	columnNames map[int]string
	columnTypes map[int]string
}

type ResultSet struct {
	pointer int
	metaData *ResultSetMetaData
	Rows []Row
}

type SQLError struct {
	error
	Code int
	Message string
}

func NewRow() Row {
	return Row{Values: make([]Cell, 0)}
}

func (row *Row) addToRow(columnName string, columnValue []byte) {
	row.Values = append(row.Values, Cell{Index:row.nextIndex(), Name:columnName, Value:columnValue})
}

func (row *Row) nextIndex() int {
	if len(row.Values) == 0 {
		return 1
	} else {
		return len(row.Values) + 1
	}
}

func (rs *ResultSet) GetMetaData() *ResultSetMetaData {
	return rs.metaData
}

func (rs *ResultSet) addRow(row Row) {
	rs.pointer = -1
	rs.Rows = append(rs.Rows, row)
}

func (rs *ResultSet) checkPointer() {
	if rs.pointer < 0 {
		panic("ResultSet is not opened. Try call Next()")
	}
}

func (rs *ResultSet) Size() int {
	return len(rs.Rows)
}

func (rs *ResultSet) Next() bool {
	rs.pointer++
	return rs.pointer < len(rs.Rows)
}

func (rs *ResultSet) MergeWith(rs2 ResultSet) {

	if rs.metaData.Hash() != rs2.metaData.Hash() {
		panic("Can't merge different metadata")
	}

	rs.pointer = -1
	for _, row2 := range rs2.Rows {
		rs.Rows = append(rs.Rows, row2)
	}
}

func (rs *ResultSet) Print() {
	rs.checkPointer()
	for _, cell := range rs.Rows[rs.pointer].Values {
		fmt.Println(rs.pointer, ":", cell.Index, cell.Name, string(cell.Value))
	}
}

func (rs *ResultSet) GetBytes(column interface{}) []byte {
	rs.checkPointer()

	index, byIndex := column.(int)
	name, byName := column.(string)

	for _, cell := range rs.Rows[rs.pointer].Values {
		if (byIndex && index == cell.Index) || (byName && name == cell.Name) {
			return cell.Value
		}
	}

	if byIndex {
		panic("There is no column with index '" + strconv.Itoa(index) + "' in ResultSet")
	} else {
		panic("There is no column with name '" + name + "' in ResultSet")
	}
}

func (rs *ResultSet) GetString(column interface{}) string {
	return string(rs.GetBytes(column))
}

func (rs *ResultSet) GetInt(column interface{}) int {
	res, err := strconv.Atoi(rs.GetString(column))

	if err != nil {
		fmt.Println(err)
	}
	return res
}

func (rs *ResultSet) GetLong(column interface{}) int64 {
	res, err := strconv.ParseInt(rs.GetString(column), 10, 64)

	if err != nil {
		fmt.Println(err)
	}
	return res
}

func (rs *ResultSet) prepareMetaData(typs []*sql.ColumnType) {

	rs.metaData = &ResultSetMetaData{columnCount:len(typs), columnNames:make(map[int]string), columnTypes:make(map[int]string)}
	for i, cType := range typs {
		rs.metaData.columnNames[i+1] = cType.Name()
		rs.metaData.columnTypes[i+1] = cType.DatabaseTypeName()
	}
}

func (rsmd *ResultSetMetaData) Hash() uint32 {
	var sb strings.Builder
	for i := 1; i <= rsmd.GetColumnCount(); i++ {
		sb.WriteString(rsmd.GetColumnName(i) + "@" + rsmd.GetColumnType(i))

		if i < rsmd.GetColumnCount() {
			sb.WriteString("|")
		}
	}

	hash := fnv.New32a()
	hash.Write([]byte(sb.String()))

	return hash.Sum32()
}

func (rsmd *ResultSetMetaData) GetColumnCount() int {
	return rsmd.columnCount
}

func (rsmd *ResultSetMetaData) GetColumnName(i int) string {
	return rsmd.columnNames[i]
}

func (rsmd *ResultSetMetaData) GetColumnType(i int) string {
	return rsmd.columnTypes[i]
}

/////////////////////// error //////////////////////////////
func answerError(err error) *SQLError {
	if err == nil {
		return nil
	}

	mysqlError, isMySQLError := err.(*mysql.MySQLError)
	if isMySQLError {
		return &SQLError{Code:int(mysqlError.Number), Message:mysqlError.Message, error:err}
	} else {
		return &SQLError{Code:0, Message:err.Error(), error:err}
	}
}

func (err *SQLError) String() string {
	if err != nil {
		return fmt.Sprintf("SQLError: %d %s", err.Code, err.Message)
	} else {
		return ""
	}
}