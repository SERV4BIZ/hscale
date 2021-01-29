package locals

import (
	"errors"
	"fmt"

	"github.com/SERV4BIZ/gfp/files"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/utility"
)

// LoadConfig is load config.json to json object.
func LoadConfig() (*jsons.JSONObject, error) {
	pathfile := fmt.Sprint(utility.GetAppDir(), utility.DS, "config.json")
	jsoConfig := jsons.JSONObjectFactory()
	jsoConfig.PutString("txt_host", "localhost")
	jsoConfig.PutInt("int_port", 3210)

	if files.ExistFile(pathfile) {
		return jsons.JSONObjectFromFile(pathfile)
	}

	return jsoConfig, errors.New("Not found config.json file")
}
