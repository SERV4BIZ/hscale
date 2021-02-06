package dbcmd

import (
	"strings"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// AddColumnList is add column to table on current database in host
func AddColumnList(driverName string, dbConn *escondb.ESCONDB, tableName string, columnName string, columnValue *jsons.JSONObject) bool {
	sql, errLoad := locals.LoadSQLDriver(driverName, "add_column_object")
	if errLoad != nil {
		panic(errLoad)
	}

	if !columnValue.CheckKey("txt_table") {
		defTable := strings.TrimPrefix(columnName, "lst_")
		columnValue.PutString("txt_table", defTable)
	}

	if !columnValue.CheckKey("int_length") {
		columnValue.PutInt("int_length", 0)
	}

	if !columnValue.CheckKey("txt_first_node") {
		columnValue.PutString("txt_first_node", "#")
	}

	if !columnValue.CheckKey("txt_last_node") {
		columnValue.PutString("txt_last_node", "#")
	}

	sql = strings.ReplaceAll(sql, "{table}", tableName)
	sql = strings.ReplaceAll(sql, "{name}", columnName)
	sql = strings.ReplaceAll(sql, "{value}", columnValue.ToString())
	_, errExec := dbConn.Exec(sql)
	if errExec != nil {
		panic(errExec)
	}

	return true
}
