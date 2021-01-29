package dbcmd

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// AddColumnDouble is add column to table on current database in host
func AddColumnDouble(driverName string, dbConn *sql.DB, tableName string, columnName string, columnValue float64) bool {
	sql, errLoad := locals.LoadSQLDriver(driverName, "add_column_double")
	handler.Panic(errLoad)

	sql = strings.ReplaceAll(sql, "{table}", tableName)
	sql = strings.ReplaceAll(sql, "{name}", columnName)
	sql = strings.ReplaceAll(sql, "{value}", fmt.Sprint(columnValue))
	_, errExec := dbConn.Exec(sql)
	handler.Panic(errExec)

	return true
}
