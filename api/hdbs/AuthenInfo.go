package hdbs

import (
	"strings"

	"github.com/SERV4BIZ/gfp/jsons"
)

// AuthenInfo is get authentication information
func (me *HDB) AuthenInfo() *jsons.JSONObject {
	jsoAuthen := jsons.JSONObjectFactory()
	jsoAuthen.PutString("txt_username", strings.TrimSpace(strings.ToLower(me.JSOConfigHost.GetString("txt_username"))))
	jsoAuthen.PutString("txt_password", strings.TrimSpace(me.JSOConfigHost.GetString("txt_password")))
	return jsoAuthen
}
