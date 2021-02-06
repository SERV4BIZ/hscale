package dbcmd

import (
	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// ListingTable is list all table in current database
func ListingTable(driverName string, dbConn *escondb.ESCONDB) []string {
	sql, errLoad := locals.LoadSQLDriver(driverName, "listing_table")
	if errLoad != nil {
		panic(errLoad)
	}

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
