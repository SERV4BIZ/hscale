package hdbs

import (
	"errors"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/networks"
)

// DataNodeListing is list all datanode
func (me *HDB) DataNodeListing() (*jsons.JSONArray, error) {
	jsoCmd := jsons.JSONObjectFactory()
	jsoCmd.PutObject("jso_authen", me.AuthenInfo())
	jsoCmd.PutString("txt_command", "datanode_listing")

	jsoReq := networks.Request(me.JSOConfigHost, jsoCmd)
	if jsoReq.GetInt("status") > 0 {
		return jsoReq.GetObject("jso_data").GetArray("jsa_listing"), nil
	}
	return nil, errors.New(jsoReq.GetString("txt_msg"))
}
