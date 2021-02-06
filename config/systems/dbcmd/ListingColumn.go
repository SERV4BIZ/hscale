package dbcmd

import (
	"strings"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// ListingColumn is list all column in table
func ListingColumn(driverName string, dbConn *escondb.ESCONDB, tableName string) []string {
	sql, errLoad := locals.LoadSQLDriver(driverName, "listing_column")
	if errLoad != nil {
		panic(errLoad)
	}

	sql = strings.ReplaceAll(sql, "{table}", tableName)

	var list []string
	rows, errQuery := dbConn.Query(sql)
	if errQuery != nil {
		panic(errQuery)
	}

	for i := 0; i < rows.Length(); i++ {
		keys := rows.GetObject(i).GetKeys()
		list = append(list, rows.GetObject(i).GetString(keys[0]))
	}

	return list
}
