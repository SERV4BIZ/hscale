package dbcmd

import (
	"database/sql"
	"strings"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// AddColumnObject is add column json object to table
func AddColumnObject(driverName string, dbConn *sql.DB, tableName string, columnName string, columnValue *jsons.JSONObject) bool {
	sql, errLoad := locals.LoadSQLDriver(driverName, "add_column_object")
	handler.Panic(errLoad)

	sql = strings.ReplaceAll(sql, "{table}", tableName)
	sql = strings.ReplaceAll(sql, "{name}", columnName)
	sql = strings.ReplaceAll(sql, "{value}", columnValue.ToString())
	_, errExec := dbConn.Exec(sql)
	handler.Panic(errExec)

	return true
}
