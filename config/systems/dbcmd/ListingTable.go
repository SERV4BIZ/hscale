package dbcmd

import (
	"database/sql"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// ListingTable is list all table in current database
func ListingTable(driverName string, dbConn *sql.DB) []string {
	sql, errLoad := locals.LoadSQLDriver(driverName, "listing_table")
	handler.Panic(errLoad)

	var list []string
	rows, errQuery := dbConn.Query(sql)
	handler.Panic(errQuery)
	defer rows.Close()

	var dbTableName string
	for rows.Next() {
		errScan := rows.Scan(&dbTableName)
		handler.Panic(errScan)
		list = append(list, dbTableName)
	}

	return list
}
