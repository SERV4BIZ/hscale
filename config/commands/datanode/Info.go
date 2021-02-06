package datanode

import (
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// Info is get info of datanode
func Info(jsoCmd *jsons.JSONObject) *jsons.JSONObject {
	jsoResult := jsons.JSONObjectFactory()
	jsoResult.PutInt("status", 0)

	nodeName := strings.TrimSpace(strings.ToLower(jsoCmd.GetString("txt_name")))
	nodeInfo, errNodeInfo := locals.LoadDataNodeInfo(nodeName)
	if errNodeInfo != nil {
		jsoResult.PutString("txt_msg", fmt.Sprint("Can not load data node info [ ", errNodeInfo, " ]"))
		return jsoResult
	}

	driverName := nodeInfo.GetObject("jso_database").GetString("txt_driver")
	jsoSQLDriver, errSQL := locals.LoadAllSQLDriver(driverName)
	if errSQL != nil {
		jsoResult.PutString("txt_msg", fmt.Sprint("Can not load sql driver [ ", errSQL, " ]"))
		return jsoResult
	}

	nodeInfo.PutObject("jso_sqldriver", jsoSQLDriver)
	jsoResult.PutObject("jso_data", nodeInfo)
	jsoResult.PutInt("status", 1)
	return jsoResult
}
