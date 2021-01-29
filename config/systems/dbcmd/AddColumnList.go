package dbcmd

import (
	"database/sql"
	"strings"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// AddColumnList is add column to table on current database in host
func AddColumnList(driverName string, dbConn *sql.DB, tableName string, columnName string, columnValue *jsons.JSONObject) bool {
	sql, errLoad := locals.LoadSQLDriver(driverName, "add_column_object")
	handler.Panic(errLoad)

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
	handler.Panic(errExec)

	return true
}
