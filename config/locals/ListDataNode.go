package locals

import (
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/filesystem"
	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/utilities"
)

// ListDataNode is listing data node
func ListDataNode() (*jsons.JSONArray, error) {
	jsaList := jsons.JSONArrayFactory()
	pathdir := fmt.Sprint(utilities.GetAppDir(), utilities.DS, "datanodes")
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
