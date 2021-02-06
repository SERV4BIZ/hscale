package dbcmd

import (
	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// ListingDatabase is list all database in host
func ListingDatabase(driverName string, dbConn *escondb.ESCONDB) []string {
	sql, errLoad := locals.LoadSQLDriver(driverName, "listing_database")
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
