package dbcmd

import (
	"strings"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// AddColumnObject is add column json object to table
func AddColumnObject(driverName string, dbConn *escondb.ESCONDB, tableName string, columnName string, columnValue *jsons.JSONObject) bool {
	sql, errLoad := locals.LoadSQLDriver(driverName, "add_column_object")
	if errLoad != nil {
		panic(errLoad)
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
