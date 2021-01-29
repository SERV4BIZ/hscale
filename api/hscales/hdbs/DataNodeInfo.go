package hdbs

import (
	"errors"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/networks"
)

// DataNodeInfo is list all datanode
func (me *HDB) DataNodeInfo(nodeName string) (*jsons.JSONObject, error) {
	jsoCmd := jsons.JSONObjectFactory()
	jsoCmd.PutObject("jso_authen", me.AuthenInfo())
	jsoCmd.PutString("txt_command", "datanode_info")
	jsoCmd.PutString("txt_name", nodeName)

	jsoReq := networks.Request(me.JSOConfigHost, jsoCmd)
	if jsoReq.GetInt("status") > 0 {
		return jsoReq.GetObject("jso_data"), nil
	}
	return nil, errors.New(jsoReq.GetString("txt_msg"))
}
