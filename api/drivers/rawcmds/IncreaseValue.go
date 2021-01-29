package rawcmds

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SERV4BIZ/hscale/api/utilities"
)

// IncreaseValue is increase value of column in database
func IncreaseValue(dbConn ConnDriver, sqlIncrease string, txtTable string, txtKeyname string, txtColumn string, dblValue float64) error {
	sqlQuery := sqlIncrease
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{modify}", fmt.Sprint(float64(time.Now().Unix())))
	sqlQuery = strings.ReplaceAll(sqlQuery, "{column}", txtColumn)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{value}", fmt.Sprint(dblValue))
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
		return errors.New("Can not increase data row")
	}

	return nil
}
