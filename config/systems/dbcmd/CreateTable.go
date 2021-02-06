package dbcmd

import (
	"strings"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// CreateTable is create table on current database in host
func CreateTable(driverName string, dbConn *escondb.ESCONDB, tableName string) bool {
	sql, errLoad := locals.LoadSQLDriver(driverName, "create_table")
	if errLoad != nil {
		panic(errLoad)
	}

	sql = strings.ReplaceAll(sql, "{name}", tableName)
	_, errExec := dbConn.Exec(sql)
	if errExec != nil {
		panic(errExec)
	}

	return true
}
