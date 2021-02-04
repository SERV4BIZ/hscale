package rawcmds

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SERV4BIZ/hscale/api/utility"
)

// IncreaseValue is increase value of column in database
func IncreaseValue(dbConn *escondb.ESCONTX, sqlIncrease string, txtTable string, txtKeyname string, txtColumn string, dblValue float64) error {
	sqlQuery := sqlIncrease
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{modify}", fmt.Sprint(float64(time.Now().Unix())))
	sqlQuery = strings.ReplaceAll(sqlQuery, "{column}", txtColumn)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{value}", fmt.Sprint(dblValue))
	sqlQuery = strings.ReplaceAll(sqlQuery, "{keyname}", utility.AddQuote(txtKeyname))
	jsoResult, errExec := dbConn.Exec(sqlQuery)
	if errExec != nil {
		return errExec
	}

	count := jsoResult.GetInt("int_affected")
	if count == 0 {
		return errors.New("Can not increase data row")
	}

	return nil
}
