package drivers

import (
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/gfp/jsons"
)

// Find is find data with conditions by sql connection
func Find(dbConn *escondb.ESCONTX, sqlFind string, txtTable string, arrColumns []string, txtConditions string, intLimit int) (*jsons.JSONArray, error) {
	if len(arrColumns) <= 0 {
		return nil, errors.New("Columns is empty")
	}

	getData := make([]interface{}, len(arrColumns))
	getDataPointers := make([]interface{}, len(arrColumns))

	txtColumns := ""
	for index, columnItem := range arrColumns {
		txtColumns = fmt.Sprint(txtColumns, columnItem, ",")
		getDataPointers[index] = &getData[index]
	}
	txtColumns = strings.TrimSpace(strings.Trim(txtColumns, ","))

	sqlQuery := sqlFind
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{columns}", txtColumns)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{conditions}", txtConditions)

	if intLimit >= 0 {
		sqlQuery = strings.ReplaceAll(sqlQuery, "{limit}", fmt.Sprint(intLimit))
	}

	jsaRows, errRows := dbConn.Query(sqlQuery)
	if errRows != nil {
		return nil, errRows
	}

	return jsaRows, nil
}
