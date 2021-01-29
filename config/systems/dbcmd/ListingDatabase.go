package dbcmd

import (
	"database/sql"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// ListingDatabase is list all database in host
func ListingDatabase(driverName string, dbConn *sql.DB) []string {
	sql, errLoad := locals.LoadSQLDriver(driverName, "listing_database")
	handler.Panic(errLoad)

	var list []string
	rows, errQuery := dbConn.Query(sql)
	handler.Panic(errQuery)
	defer rows.Close()

	var dbName string
	for rows.Next() {
		errScan := rows.Scan(&dbName)
		handler.Panic(errScan)
		list = append(list, dbName)
	}

	return list
}
