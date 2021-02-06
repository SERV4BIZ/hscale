package dbcmd

import (
	"strings"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// CreateDatabase is create database in host
func CreateDatabase(driverName string, dbConn *escondb.ESCONDB, dbName string) bool {
	sql, errLoad := locals.LoadSQLDriver(driverName, "create_database")
	if errLoad != nil {
		panic(errLoad)
	}

	sql = strings.ReplaceAll(sql, "{name}", dbName)
	_, errExec := dbConn.Exec(sql)
	if errExec != nil {
		panic(errExec)
	}

	return true
}
