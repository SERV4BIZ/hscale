package rawcmds

import (
	"errors"
	"strings"

	"github.com/SERV4BIZ/hscale/api/utility"
)

// DeleteRow is get data by sql connection
func DeleteRow(dbConn *escondb.ESCONTX, sqlDelete string, txtTable string, txtKeyname string) error {
	sqlQuery := sqlDelete
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{keyname}", utility.AddQuote(txtKeyname))
	jsoResult, errExec := dbConn.Exec(sqlQuery)
	if errExec != nil {
		return errExec
	}

	count := jsoResult.GetInt("int_affected")
	if count == 0 {
		return errors.New("Can not update data row")
	}

	return nil
}
