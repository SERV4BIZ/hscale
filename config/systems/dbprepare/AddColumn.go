package dbprepare

import (
	"strings"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/systems/dbcmd"
)

// AddColumn is check column all in table if not found add it.
func AddColumn(dbConn *escondb.ESCONDB, driverName string, dbName string, dbTable string, jsoColumn *jsons.JSONObject) {
	listCols := dbcmd.ListingColumn(driverName, dbConn, dbTable)
	columns := jsoColumn.GetKeys()
	for _, pcolName := range columns {
		blnFound := false
		for _, hostCol := range listCols {
			if strings.ToLower(pcolName) == strings.ToLower(hostCol) {
				blnFound = true
				break
			}
		}

		if !blnFound {
			pcol := strings.ToLower(strings.TrimSpace(pcolName))
			if strings.HasPrefix(pcol, "txt_") {
				dbcmd.AddColumnText(driverName, dbConn, dbTable, pcol, jsoColumn.GetString(pcolName))
			} else if strings.HasPrefix(pcol, "int_") {
				dbcmd.AddColumnInteger(driverName, dbConn, dbTable, pcol, jsoColumn.GetInt(pcolName))
			} else if strings.HasPrefix(pcol, "dbl_") {
				dbcmd.AddColumnDouble(driverName, dbConn, dbTable, pcol, jsoColumn.GetDouble(pcolName))
			} else if strings.HasPrefix(pcol, "bln_") {
				dbcmd.AddColumnBoolean(driverName, dbConn, dbTable, pcol, jsoColumn.GetBool(pcolName))
			} else if strings.HasPrefix(pcol, "jsa_") {
				dbcmd.AddColumnArray(driverName, dbConn, dbTable, pcol, jsoColumn.GetArray(pcolName))
			} else if strings.HasPrefix(pcol, "jso_") {
				dbcmd.AddColumnObject(driverName, dbConn, dbTable, pcol, jsoColumn.GetObject(pcolName))
			} else if strings.HasPrefix(pcol, "lst_") {
				dbcmd.AddColumnList(driverName, dbConn, dbTable, pcol, jsoColumn.GetObject(pcolName))
			}
		}
	}
}
