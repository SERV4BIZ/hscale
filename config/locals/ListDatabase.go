package locals

import (
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/filesystem"
	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/utilities"
)

// ListDatabase is listing database all
func ListDatabase() (*jsons.JSONArray, error) {
	jsaList := jsons.JSONArrayFactory()
	pathdir := fmt.Sprint(utilities.GetAppDir(), utilities.DS, "databases")
	files, err := filesystem.ScanDir(pathdir)
	if handler.Error(err) {
		return nil, err
	}

	for _, fileName := range files {
		if strings.HasSuffix(fileName, ".json") {
			jsaList.PutString(strings.TrimSpace(strings.ToLower(strings.TrimSuffix(fileName, ".json"))))
		}
	}
	return jsaList, err
}
