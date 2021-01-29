package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/global"

	command_database "github.com/SERV4BIZ/hscale/config/commands/database"
	command_datanode "github.com/SERV4BIZ/hscale/config/commands/datanode"
	command_network "github.com/SERV4BIZ/hscale/config/commands/network"
)

// WorkHandler is main job for any request
func WorkHandler(w http.ResponseWriter, r *http.Request) {
	<-time.After(time.Millisecond)

	global.MutexState.Lock()
	global.CountState++
	global.MutexState.Unlock()

	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024*100)
	defer r.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	jsoResult := jsons.JSONObjectFactory()
	jsoResult.PutInt("status", 0)

	buffer, errBody := ioutil.ReadAll(r.Body)
	if handler.Error(errBody) {
		jsoResult.PutString("txt_msg", fmt.Sprint("Can not read body from http request [ ", errBody, " ]"))
	} else {
		jsoCmd, errCmd := jsons.JSONObjectFromString(string(buffer))
		if handler.Error(errCmd) {
			jsoResult.PutString("txt_msg", fmt.Sprint("Can not load command from json string buffer [ ", errCmd, " ]"))
		} else {
			jsoAuthen := jsoCmd.GetObject("jso_authen")
			authenUser := strings.TrimSpace(strings.ToLower(jsoAuthen.GetString("txt_username")))
			authenPass := strings.TrimSpace(jsoAuthen.GetString("txt_password"))

			if len(authenUser) > 0 {
				if global.Username == authenUser && global.Password == authenPass {
					switch jsoCmd.GetString("txt_command") {
					case "network_ping":
						jsoResult = command_network.Ping(jsoCmd)
					case "datanode_info":
						jsoResult = command_datanode.Info(jsoCmd)
					case "datanode_listing":
						jsoResult = command_datanode.Listing(jsoCmd)
					case "database_info":
						jsoResult = command_database.Info(jsoCmd)
					case "database_listing":
						jsoResult = command_database.Listing(jsoCmd)
					}
				}
			}
		}
	}
	w.Write([]byte(jsoResult.ToString()))
}
