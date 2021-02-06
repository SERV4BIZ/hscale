package dbcmd

import (
	"fmt"
	"strings"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// AddColumnDouble is add column to table on current database in host
func AddColumnDouble(driverName string, dbConn *escondb.ESCONDB, tableName string, columnName string, columnValue float64) bool {
	sql, errLoad := locals.LoadSQLDriver(driverName, "add_column_double")
	if errLoad != nil {
		panic(errLoad)
	}

	sql = strings.ReplaceAll(sql, "{table}", tableName)
	sql = strings.ReplaceAll(sql, "{name}", columnName)
	sql = strings.ReplaceAll(sql, "{value}", fmt.Sprint(columnValue))
	_, errExec := dbConn.Exec(sql)
	if errExec != nil {
		panic(errExec)
	}

	return true
}
