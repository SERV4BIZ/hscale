package drivers

import (
	"strings"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/hscale/api/utility"
)

// DeleteRow is get data by sql connection
func DeleteRow(dbConn *escondb.ESCONTX, sqlDelete string, txtTable string, txtKeyname string) error {
	sqlQuery := sqlDelete
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{keyname}", utility.AddQuote(txtKeyname))
	_, errExec := dbConn.Exec(sqlQuery)
	if errExec != nil {
		return errExec
	}
	return nil
}
