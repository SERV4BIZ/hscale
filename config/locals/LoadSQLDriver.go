package locals

import (
	"errors"
	"fmt"

	"github.com/SERV4BIZ/gfp/files"
	"github.com/SERV4BIZ/hscale/config/utility"
)

// LoadSQLDriver is load sql driver
func LoadSQLDriver(driver string, sqlfile string) (string, error) {
	pathfile := fmt.Sprint(utility.GetAppDir(), utility.DS, "sqldrivers", utility.DS, driver, utility.DS, sqlfile, ".sql")
	if files.ExistFile(pathfile) {
		buffer, err := files.ReadFile(pathfile)
		return string(buffer), err
	}
	return "", errors.New("Not found SQL Driver")
}
