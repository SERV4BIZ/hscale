package rawcmds

import (
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/utility"
)

// GetRow is get data by sql connection
func GetRow(dbConn *escondb.ESCONTX, sqlSelect string, txtTable string, arrColumns []string, txtKeyname string) (*jsons.JSONObject, error) {
	if len(arrColumns) <= 0 {
		return nil, errors.New("Columns is empty")
	}

	txtColumns := ""
	for index, columnItem := range arrColumns {
		txtColumns = fmt.Sprint(txtColumns, columnItem, ",")
		getDataPointers[index] = &getData[index]
	}
	txtColumns = strings.TrimSpace(strings.Trim(txtColumns, ","))

	sqlQuery := sqlSelect
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{columns}", txtColumns)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{keyname}", utility.AddQuote(txtKeyname))
	jsoResult ,errGet := dbConn.Fetch(sqlQuery)
	if errGet != nil {
		return nil, errGet
	}
	
	return jsoResult, nil
}
