package dbcmd

import (
	"database/sql"
	"strings"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// CreateDatabase is create database in host
func CreateDatabase(driverName string, dbConn *sql.DB, dbName string) bool {
	sql, errLoad := locals.LoadSQLDriver(driverName, "create_database")
	handler.Panic(errLoad)

	sql = strings.ReplaceAll(sql, "{name}", dbName)
	_, errExec := dbConn.Exec(sql)
	handler.Panic(errExec)

	return true
}
