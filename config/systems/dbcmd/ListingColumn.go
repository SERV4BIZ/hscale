package dbcmd

import (
	"database/sql"
	"strings"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// ListingColumn is list all column in table
func ListingColumn(driverName string, dbConn *sql.DB, tableName string) []string {
	sql, errLoad := locals.LoadSQLDriver(driverName, "listing_column")
	handler.Panic(errLoad)

	sql = strings.ReplaceAll(sql, "{table}", tableName)

	var list []string
	rows, errQuery := dbConn.Query(sql)
	handler.Panic(errQuery)
	defer rows.Close()

	var dbColName string
	for rows.Next() {
		errScan := rows.Scan(&dbColName)
		handler.Panic(errScan)
		list = append(list, dbColName)
	}

	return list
}
