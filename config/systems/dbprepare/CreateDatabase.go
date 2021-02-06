package dbprepare

import (
	"strings"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/locals"
	"github.com/SERV4BIZ/hscale/config/systems/dbcmd"
)

// CreateDatabase is check database is has in host if not found create it.
func CreateDatabase(jsoNodeinfo *jsons.JSONObject, jsoDatabase *jsons.JSONObject, driverName string, dbConn *escondb.ESCONDB) {
	listdbs := dbcmd.ListingDatabase(driverName, dbConn)
	jsaDatabase, errList := locals.ListDatabase()
	if errList != nil {
		panic(errList)
	}

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
			dbcmd.CreateDatabase(driverName, dbConn, dbName)
		}

		// Create Table
		CreateTable(jsoNodeinfo, jsoDatabase, driverName, dbName)
	}
}
