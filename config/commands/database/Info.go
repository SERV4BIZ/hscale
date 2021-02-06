package database

import (
	"fmt"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// Info is get info database
func Info(jsoCmd *jsons.JSONObject) *jsons.JSONObject {
	jsoResult := jsons.JSONObjectFactory()
	jsoResult.PutInt("status", 0)

	dbname := jsoCmd.GetString("txt_name")
	dbinfo, err := locals.LoadDatabaseInfo(dbname)
	if err != nil {
		jsoResult.PutString("txt_msg", fmt.Sprint("Can not load database info [ ", err, " ]"))
		return jsoResult
	}

	// Counters Table
	jsoCounter := jsons.JSONObjectFactory()
	jsoCounter.PutInt("int_value", 0)
	dbinfo.GetObject("jso_schema").PutObject("counters", jsoCounter)

	// Nodes Table
	jsoNode := jsons.JSONObjectFactory()
	jsoNode.PutInt("int_length", 0)
	jsoNode.PutString("txt_owner_keyname", "")
	jsoNode.PutString("txt_owner_table", "")
	jsoNode.PutString("txt_table", "")
	jsoNode.PutString("txt_back_node", "")
	jsoNode.PutString("txt_next_node", "")
	jsoNode.PutArray("jsa_listing", jsons.JSONArrayFactory())
	dbinfo.GetObject("jso_schema").PutObject("nodes", jsoNode)

	jsoResult.PutObject("jso_data", dbinfo)
	jsoResult.PutInt("status", 1)
	return jsoResult
}
