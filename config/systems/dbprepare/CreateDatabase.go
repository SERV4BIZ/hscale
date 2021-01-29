package dbprepare

import (
	"database/sql"
	"strings"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/locals"
	"github.com/SERV4BIZ/hscale/config/systems/dbcmd"
)

// CreateDatabase is check database is has in host if not found create it.
func CreateDatabase(jsoNodeinfo *jsons.JSONObject, jsoDatabase *jsons.JSONObject, driverName string, conn *sql.DB) {
	listdbs := dbcmd.ListingDatabase(driverName, conn)
	jsaDatabase, errList := locals.ListDatabase()
	handler.Panic(errList)

	for i := 0; i < jsaDatabase.Length(); i++ {
		dbName := strings.ToLower(strings.TrimSpace(jsaDatabase.GetString(i)))
		blnFound := false
		for _, val := range listdbs {
			if dbName == val {
				blnFound = true
				break
			}
		}
		if !blnFound {
			dbcmd.CreateDatabase(driverName, conn, dbName)
		}

		// Create Table
		CreateTable(jsoNodeinfo, jsoDatabase, driverName, dbName)
	}
}
