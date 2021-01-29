package locals

import (
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/filesystem"
	"github.com/SERV4BIZ/gfp/handler"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/utilities"
)

// LoadAllSQLDriver is load list sql driver
func LoadAllSQLDriver(driver string) (*jsons.JSONObject, error) {
	jsoSQLDriver := jsons.JSONObjectFactory()
	pathdir := fmt.Sprint(utilities.GetAppDir(), utilities.DS, "sqldrivers", utilities.DS, driver)
	files, err := filesystem.ScanDir(pathdir)
	if handler.Error(err) {
		return nil, err
	}

	for _, fileName := range files {
		if strings.HasSuffix(fileName, ".sql") {
			pureName := strings.TrimSpace(strings.ToLower(strings.TrimSuffix(fileName, ".sql")))
			fullPath := fmt.Sprint(pathdir, utilities.DS, fileName)
			buff, err := filesystem.ReadFile(fullPath)
			if handler.Error(err) {
				return nil, err
			}

			jsoSQLDriver.PutString(pureName, string(buff))
		}
	}
	return jsoSQLDriver, err
}
