package dbcmd

import (
	"database/sql"
	"strings"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// CreateTable is create table on current database in host
func CreateTable(driverName string, dbConn *sql.DB, tableName string) bool {
	sql, errLoad := locals.LoadSQLDriver(driverName, "create_table")
	handler.Panic(errLoad)

	sql = strings.ReplaceAll(sql, "{name}", tableName)
	_, errExec := dbConn.Exec(sql)
	handler.Panic(errExec)

	return true
}
