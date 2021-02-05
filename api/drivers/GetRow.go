package drivers

import (
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/utility"
)

// GetRow is get data by sql connection
func GetRow(dbConn *escondb.ESCONTX, sqlSelect string, txtTable string, arrColumns []string, txtKeyname string) (*jsons.JSONObject, error) {
	if len(arrColumns) <= 0 {
		return nil, errors.New("Columns is empty")
	}

	txtColumns := ""
	for _, columnItem := range arrColumns {
		txtColumns = fmt.Sprint(txtColumns, columnItem, ",")
	}
	txtColumns = strings.TrimSpace(strings.Trim(txtColumns, ","))

	sqlQuery := sqlSelect
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{columns}", txtColumns)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{keyname}", utility.AddQuote(txtKeyname))
	jsoRow, errFetch := dbConn.Fetch(sqlQuery)
	if errFetch != nil {
		return nil, errFetch
	}

	return jsoRow, nil
}
