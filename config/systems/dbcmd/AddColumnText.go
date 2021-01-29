package dbcmd

import (
	"database/sql"
	"strings"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// AddColumnText is add column to table on current database in host
func AddColumnText(driverName string, dbConn *sql.DB, tableName string, columnName string, columnValue string) bool {
	sql, errLoad := locals.LoadSQLDriver(driverName, "add_column_text")
	handler.Panic(errLoad)

	sql = strings.ReplaceAll(sql, "{table}", tableName)
	sql = strings.ReplaceAll(sql, "{name}", columnName)
	sql = strings.ReplaceAll(sql, "{value}", columnValue)
	_, errExec := dbConn.Exec(sql)
	handler.Panic(errExec)

	return true
}
