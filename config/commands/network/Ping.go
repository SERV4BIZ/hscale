package network

import (
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/global"
)

// Ping is command check network status
func Ping(jsoCmd *jsons.JSONObject) *jsons.JSONObject {
	jsoResult := jsons.JSONObjectFactory()
	jsoResult.PutInt("status", 1)

	global.MutexState.RLock()
	jsoData := jsons.JSONObjectFactory()
	jsoData.PutInt("int_memory", global.MemoryState)
	jsoData.PutInt("int_load", global.LoadState)
	jsoResult.PutObject("jso_data", jsoData)
	global.MutexState.RUnlock()

	return jsoResult
}
