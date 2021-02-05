package drivers

import (
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/gfp/jsons"
)

// Listing is list data by sql connection
func Listing(dbConn *escondb.ESCONTX, sqlListing string, txtTable string, arrColumns []string, intOffset int, intLimit int) (*jsons.JSONArray, error) {
	if len(arrColumns) <= 0 {
		return nil, errors.New("Column data is empty")
	}

	getData := make([]interface{}, len(arrColumns))
	getDataPointers := make([]interface{}, len(arrColumns))

	txtColumns := ""
	for index, columnItem := range arrColumns {
		txtColumns = fmt.Sprint(txtColumns, columnItem, ",")
		getDataPointers[index] = &getData[index]
	}
	txtColumns = strings.TrimSpace(strings.Trim(txtColumns, ","))

	sqlQuery := sqlListing
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{columns}", txtColumns)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{offset}", fmt.Sprint(intOffset))
	sqlQuery = strings.ReplaceAll(sqlQuery, "{limit}", fmt.Sprint(intLimit))
	jsaRow, errQuery := dbConn.Query(sqlQuery)
	if errQuery != nil {
		return nil, errQuery
	}

	return jsaRow, nil

}
