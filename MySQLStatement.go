package gosql

import (
	"database/sql"
	"strconv"
	"gosql/utils"
	"strings"
	"fmt"
	"time"
)

func (db *MySQLConnection) Transaction() *SQLError {

	if db.isTransaction {
		panic("Already in transaction")
	}

	tx, err := db.dataBase.Begin()
	if err == nil {
		db.transaction = tx
		db.isTransaction = true
	}
	return answerError(err)
}

func (db *MySQLConnection) Commit() *SQLError {
	defer func() {
		db.isTransaction = false
		db.transaction = nil
	}()
	if !db.isTransaction {
		panic("Transaction is closed")
	}
	err := db.transaction.Commit()
	return answerError(err)
}

func (db *MySQLConnection) Rollback() *SQLError {
	defer func() {
		db.isTransaction = false
		db.transaction = nil
	}()
	if !db.isTransaction {
		panic("Transaction is closed")
	}
	err := db.transaction.Rollback()
	return answerError(err)
}

// returns native *sql.Rows
func (db *MySQLConnection) SelectNative(query string, params ... interface{}) (*sql.Rows, *SQLError) {
	db.checkConnection()

	paramPointers := make([]interface{}, len(params))
	for i := range params {
		paramPointers[i] = &params[i]
	}

	var res *sql.Rows; var err error
	if db.isTransaction {
		res, err = db.transaction.Query(query, paramPointers...)
	} else {
		res, err = db.dataBase.Query(query, paramPointers...)
	}
	defer res.Close()

	if err == nil {
		return res, nil
	} else {
		return nil, answerError(err)
	}
}

// returns ResutSet struct like an java object
func (db *MySQLConnection) Select(query string, params ... interface{}) (ResultSet, *SQLError) {
	db.checkConnection()

	var result ResultSet
	paramPointers := make([]interface{}, len(params))
	for i := range params {
		paramPointers[i] = &params[i]
	}

	var rows *sql.Rows; var err error
	if db.isTransaction {
		rows, err = db.transaction.Query(query, paramPointers...)
	} else {
		rows, err = db.dataBase.Query(query, paramPointers...)
	}
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
				return result, answerError(err)
			}

			row := NewRow()
			for i, columnName := range columns {
				value := valPointers[i].(*[]byte)
				row.addToRow(columnName, *value)
			}
			result.addRow(row)
		}
	}
	return result, answerError(err)
}

func (db *MySQLConnection) ExecuteInsert(query string, params ... interface{}) (int64, *SQLError) {

	paramPointers := make([]interface{}, len(params))
	for i := range params {
		paramPointers[i] = &params[i]
	}

	var res sql.Result; var err error
	if db.isTransaction {
		res, err = db.transaction.Exec(query, paramPointers...)
	} else {
		res, err = db.dataBase.Exec(query, paramPointers...)
	}

	if err == nil {
		lastID, err := res.LastInsertId()
		return lastID, answerError(err)
	}
	return 0, answerError(err)
}

func (db *MySQLConnection) ExecuteUpdate(query string, params ... interface{}) (int64, *SQLError) {

	paramPointers := make([]interface{}, len(params))
	for i := range params {
		paramPointers[i] = &params[i]
	}

	var res sql.Result; var err error
	if db.isTransaction {
		res, err = db.transaction.Exec(query, paramPointers...)
	} else {
		res, err = db.dataBase.Exec(query, paramPointers...)
	}
	if err == nil {
		affRows, err := res.RowsAffected()
		return affRows, answerError(err)
	}
	return 0, answerError(err)
}

func (db *MySQLConnection) ExecuteCall(procedureName string, params ... interface{}) (ResultSet, *SQLError) {

	paramPointers := make([]interface{}, len(params))
	for i := range params {
		paramPointers[i] = &params[i]
	}

	paramCalls := ""
	if len(params) > 0 {
		paramCalls = strings.Repeat("?,", len(params))[:len(params)*2 - 1]
	}

	query := fmt.Sprintf("CALL `%s`(%s);", procedureName, paramCalls)

	res, err := db.Select(query, paramPointers...)
	if err == nil {
		return res, nil
	}
	return ResultSet{}, err
}

//////////////////////////////////////////////////////////////
/////////////////////// DUMPING DATA /////////////////////////
//////////////////////////////////////////////////////////////
func (db *MySQLConnection) DumpByQuery(query string, tables ... string) ([]string, *SQLError) {
	rs, err := db.Select(query)
	if err == nil {
		var result []string
		fieldName := rs.GetMetaData().GetColumnName(1)
		for rs.Next() {
			whereCond := fmt.Sprintf("%s='%s'", fieldName, rs.GetString(fieldName))
			for _, tableName := range tables {
				dmp, err2 := db.DumpTable(tableName, whereCond, 0)
				if err2 == nil {
					result = append(result, dmp...)
				} else {
					err = err2
				}
			}
		}
		return result, nil
	} else {
		return nil, err
	}
}

func (db *MySQLConnection) DumpTable(tableName, whereCond string, limit int) ([]string, *SQLError) {

	if db.isTransaction {
		panic("Dumping in transaction not possible")
	}

	// prepare conditions for WHERE and LIMIT
	limitCond := ""
	if limit > 0 {limitCond = " LIMIT " + strconv.Itoa(limit)}
	if len(whereCond) > 0 {whereCond = " WHERE " + whereCond}

	rs, err := db.Select("SELECT * FROM " + tableName + whereCond + limitCond)

	if err == nil {
		var result []string
		md := rs.GetMetaData()

		for rs.Next() {
			values := make([]string, md.GetColumnCount())
			//fields := make([]string, md.GetColumnCount())
			for rsIndx,sliceIndx := 1,0; rsIndx <= md.GetColumnCount(); rsIndx,sliceIndx = rsIndx+1,sliceIndx+1 {
				varType := strings.ToLower(md.GetColumnType(rsIndx))
				if strings.Contains(varType, "int") {
					values[sliceIndx] = utils.PrepareStringValue(rs.GetLong(rsIndx))
				} else if strings.Contains(varType, "blob") {
					values[sliceIndx] = utils.PrepareStringValue(rs.GetBytes(rsIndx))
				} else {
					values[sliceIndx] = utils.PrepareStringValue(rs.GetString(rsIndx))
				}

				//fields[sliceIndx] = md.columnNames[rsIndx]
			}

			insert := db.Insert(tableName)
			//insert.fields = fields
			insert.values = append(insert.values, utils.ConcatValues("(", values, ",", ")"))

			result = append(result, insert.String() + ";")
		}
		return result, nil
	} else {
		return nil, err
	}
}

type tableField struct {
	fName string
	fValue string
}

//////////////////////////////////////////////////////////////
///////////////////// INSERT & REPLACE ///////////////////////
//////////////////////////////////////////////////////////////
type InsertStmt struct {
	db *MySQLConnection
	table string
	fields []string
	values []string
	lastInsertID int64
	replace bool
}

func (db *MySQLConnection) Insert(table string) *InsertStmt {
	return &InsertStmt{db:db, table:table, replace:false}
}

func (db *MySQLConnection) Replace(table string) *InsertStmt {
	return &InsertStmt{db:db, table:table, replace:true}
}

func (in *InsertStmt) Fields(fields ...string) *InsertStmt {
	in.fields = make([]string, len(fields))
	for i, field := range fields {
		in.fields[i] = field
	}
	return in
}

func (in *InsertStmt) Value(values ...interface{}) *InsertStmt {
	strValues := make([]string, len(values))
	for i, val := range values {
		strValues[i] = utils.PrepareStringValue(val)
	}

	in.values = append(in.values, utils.ConcatValues("(", strValues, ",", ")"))
	return in
}

func (in *InsertStmt) build() string {
	pFields := ""
	if len(in.fields) > 0 {
		pFields = utils.ConcatValues(" (", in.fields, ",", ")")
	}

	if len(in.values) == 0 {
		panic("Insert VALUES are not specified")
	}

	return "INSERT INTO " + in.table + pFields + " VALUES " + utils.ConcatValues("", in.values, ",", "")
}

func (in *InsertStmt) Run() (int64, *SQLError) {
	var result sql.Result; var err error
	if in.db.isTransaction {
		result, err = in.db.transaction.Exec(in.build())
	} else {
		result, err = in.db.dataBase.Exec(in.build())
	}

	sqlErr := answerError(err)
	if sqlErr == nil {
		in.lastInsertID, _ = result.LastInsertId()
		affRows, err := result.RowsAffected()
		return affRows, answerError(err)
	} else {
		if in.replace  && sqlErr.Code == 1062 {
			replaceSQL := strings.Replace(in.build(), "INSERT", "REPLACE", 1)
			return in.db.ExecuteUpdate(replaceSQL)
		}
	}
	return 0, sqlErr
}

func (in *InsertStmt) ReturnGeneratedKey() int64 {
	return in.lastInsertID
}

func (in *InsertStmt) String() string {
	return in.build()
}

//////////////////////////////////////////////////////////////
////////////////////////// UPDATE ////////////////////////////
//////////////////////////////////////////////////////////////
type UpdateStmt struct {
	db *MySQLConnection
	table string
	setFields []tableField
	whereFields []tableField
	limit int
}

func (db *MySQLConnection) Update(table string) *UpdateStmt {
	return &UpdateStmt{db:db, table:table}
}

func (up *UpdateStmt) SetField(fName string, fValue interface{}) *UpdateStmt {
	up.setFields = append(up.setFields, tableField{fName:fName, fValue:utils.PrepareStringValue(fValue)})
	return up
}

func (up *UpdateStmt) WhereField(fName string, fValue interface{}) *UpdateStmt {
	up.whereFields = append(up.whereFields, tableField{fName:fName, fValue:utils.PrepareStringValue(fValue)})
	return up
}

func (up *UpdateStmt) Limit(limit int) *UpdateStmt {
	up.limit = limit
	return up
}

func (up *UpdateStmt) build() string {
	setFields := make([]string, len(up.setFields))
	whereFields := make([]string, len(up.whereFields))

	for i, field := range up.setFields {
		setFields[i] = field.fName + "=" + field.fValue
	}

	for i, field := range up.whereFields {
		whereFields[i] = field.fName + "=" + field.fValue
	}

	strSet := utils.ConcatValues(" SET ", setFields, ", ", "")
	strWhere := utils.ConcatValues(" WHERE ", whereFields, " AND ", "")
	strLimit := " LIMIT " + strconv.Itoa(up.limit)
	return "UPDATE " + up.table + strSet + strWhere + strLimit
}

func (up *UpdateStmt) Run() (int64, *SQLError) {
	return up.db.ExecuteUpdate(up.build())
}

func (up *UpdateStmt) String() string {
	return up.build()
}

//////////////////////////////////////////////////////////////
///////////////////////// HANDLER ////////////////////////////
//////////////////////////////////////////////////////////////
type MySQLHandler struct {
	db MySQLConnection
	alias string
	hasNext bool
}

func (db *MySQLConnection) CreateMySQLHandler(table string) *MySQLHandler {
	handlerConn, err := CreateMySQLConnection(db.Config)
	if err == nil {

		alias := fmt.Sprintf("H_%s_%d", table, time.Now().Unix())
		query := fmt.Sprintf("HANDLER %s OPEN AS %s", table, alias)

		_, err := handlerConn.ExecuteUpdate(query)

		if err == nil {
			return &MySQLHandler{db:handlerConn, alias:alias, hasNext:true}
		}
	}
	panic(err)
}

func (handler *MySQLHandler) Close() {
	handler.db.ExecuteUpdate(fmt.Sprintf("HANDLER %s CLOSE", handler.alias))
	handler.db.Close()
}

func (handler *MySQLHandler) HasNext() bool {
	return handler.hasNext
}

func (handler *MySQLHandler) Next(condition, keyName string, limit int) ResultSet {
	return handler.read(condition, keyName, limit, "NEXT")
}

func (handler *MySQLHandler) Prev(condition, keyName string, limit int) ResultSet {
	if len(keyName) == 0 {
		keyName = "PRIMARY"
	}
	return handler.read(condition, keyName, limit, "PREV")
}

func (handler *MySQLHandler) read(condition, keyName string, limit int, operator string) ResultSet {

	if len(condition) > 0 {
		condition = fmt.Sprintf("WHERE %s", condition)
	}

	if len(keyName) > 0 {
		keyName = fmt.Sprintf("`%s`", keyName)
	}

	if limit <= 0 || limit > 10000 {
		fmt.Println("Limit range is between 1 and 10000.. Using default limit - 1000")
		limit = 1000
	}

	query := fmt.Sprintf("HANDLER %s READ %s %s %s LIMIT %d", handler.alias, keyName, operator, condition, limit)
	rs, err := handler.db.Select(query)

	if err != nil {
		panic(err)
	}

	if rs.Size() == 0 {
		handler.hasNext = false
	}

	return rs
}