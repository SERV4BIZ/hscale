package locals

import (
	"errors"
	"fmt"

	"github.com/SERV4BIZ/gfp/filesystem"
	"github.com/SERV4BIZ/hscale/config/utilities"
)

// LoadSQLDriver is load sql driver
func LoadSQLDriver(driver string, sqlfile string) (string, error) {
	pathfile := fmt.Sprint(utilities.GetAppDir(), utilities.DS, "sqldrivers", utilities.DS, driver, utilities.DS, sqlfile, ".sql")
	if filesystem.ExistFile(pathfile) {
		buffer, err := filesystem.ReadFile(pathfile)
		return string(buffer), err
	}
	return "", errors.New("Not found SQL Driver")
}
