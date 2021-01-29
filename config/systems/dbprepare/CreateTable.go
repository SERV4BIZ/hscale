package dbprepare

import (
	"strings"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/locals"
	"github.com/SERV4BIZ/hscale/config/systems/dbcmd"
)

// CreateTable is check table and shard number on database if not found create it.
func CreateTable(jsoNodeinfo *jsons.JSONObject, jsoDatabase *jsons.JSONObject, driverName string, dbName string) {
	connTable, errConnTable := Connect(driverName, jsoDatabase.GetString("txt_host"), jsoDatabase.GetInt("int_port"), jsoDatabase.GetString("txt_username"), jsoDatabase.GetString("txt_password"), dbName)
	handler.Panic(errConnTable)
	defer connTable.Close()

	listTables := dbcmd.ListingTable(driverName, connTable)
	jsoDBInfo, errDBInfo := locals.LoadDatabaseInfo(dbName)
	handler.Panic(errDBInfo)

	jsoSchema := jsoDBInfo.GetObject("jso_schema")

	// Counters Table
	jsoCounter := jsons.JSONObjectFactory()
	jsoCounter.PutInt("int_value", 0)
	jsoSchema.PutObject("counters", jsoCounter)

	// Nodes Table
	jsoNode := jsons.JSONObjectFactory()
	jsoNode.PutInt("int_length", 0)
	jsoNode.PutString("txt_owner_keyname", "")
	jsoNode.PutString("txt_owner_table", "")
	jsoNode.PutString("txt_table", "")
	jsoNode.PutString("txt_back_node", "")
	jsoNode.PutString("txt_next_node", "")
	jsoNode.PutArray("jsa_listing", jsons.JSONArrayFactory())
	jsoSchema.PutObject("nodes", jsoNode)

	schemas := jsoSchema.GetKeys()
	for _, schemaName := range schemas {
		txtTableName := strings.ToLower(strings.TrimSpace(schemaName))
		blnFound := false
		for _, val := range listTables {
			if txtTableName == val {
				blnFound = true
				break
			}
		}

		if !blnFound {
			dbcmd.CreateTable(driverName, connTable, txtTableName)
		}

		// Add Column
		AddColumn(connTable, driverName, dbName, txtTableName, jsoSchema.GetObject(schemaName))
	}
}
