package locals

import (
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/files"
	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/utility"
)

// LoadAllSQLDriver is load list sql driver
func LoadAllSQLDriver(driver string) (*jsons.JSONObject, error) {
	jsoSQLDriver := jsons.JSONObjectFactory()
	pathdir := fmt.Sprint(utility.GetAppDir(), utility.DS, "sqldrivers", utility.DS, driver)
	filelist, err := files.ScanDir(pathdir)
	if handler.Error(err) {
		return nil, err
	}

	for _, fileName := range filelist {
		if strings.HasSuffix(fileName, ".sql") {
			pureName := strings.TrimSpace(strings.ToLower(strings.TrimSuffix(fileName, ".sql")))
			fullPath := fmt.Sprint(pathdir, utility.DS, fileName)
			buff, err := files.ReadFile(fullPath)
			if handler.Error(err) {
				return nil, err
			}

			jsoSQLDriver.PutString(pureName, string(buff))
		}
	}
	return jsoSQLDriver, err
}
