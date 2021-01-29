package rawcmds

import (
	"errors"
	"strings"

	"github.com/SERV4BIZ/hscale/api/utilities"
)

// DeleteRow is get data by sql connection
func DeleteRow(dbConn ConnDriver, sqlDelete string, txtTable string, txtKeyname string) error {
	sqlQuery := sqlDelete
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{keyname}", utilities.AddQuote(txtKeyname))
	dbResult, errExec := dbConn.Exec(sqlQuery)
	if errExec != nil {
		return errExec
	}

	count, errResult := dbResult.RowsAffected()
	if errResult != nil {
		return errResult
	}

	if count == 0 {
		return errors.New("Can not update data row")
	}

	return nil
}
