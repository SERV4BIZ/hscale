package locals

import (
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/filesystem"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/utilities"
)

// LoadDatabaseInfo is load database info
func LoadDatabaseInfo(name string) (*jsons.JSONObject, error) {
	pathfile := fmt.Sprint(utilities.GetAppDir(), utilities.DS, "databases", utilities.DS, strings.TrimSpace(strings.ToLower(name)), ".json")
	if filesystem.ExistFile(pathfile) {
		return jsons.JSONObjectFromFile(pathfile)
	}
	return nil, errors.New(fmt.Sprint("Not found ", pathfile, " file"))
}
