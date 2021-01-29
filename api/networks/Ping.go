package networks

import (
	"strings"

	"github.com/SERV4BIZ/gfp/jsons"
)

// Ping is check network status
func Ping(jsoHost *jsons.JSONObject) *jsons.JSONObject {
	jsoAuthen := jsons.JSONObjectFactory()
	jsoAuthen.PutString("txt_username", strings.TrimSpace(strings.ToLower(jsoHost.GetString("txt_username"))))
	jsoAuthen.PutString("txt_password", strings.TrimSpace(jsoHost.GetString("txt_password")))

	jsoCmd := new(jsons.JSONObject).Factory()
	jsoCmd.PutString("txt_command", "network_ping")

	jsoCmd.PutObject("jso_authen", jsoAuthen)
	return Request(jsoHost, jsoCmd)
}
