package rawcmds

import (
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/jsons"
)

// Listing is list data by sql connection
func Listing(dbConn ConnDriver, sqlListing string, txtTable string, arrColumns []string, intOffset int, intLimit int) (*jsons.JSONArray, error) {
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
	dbRows, errQuery := dbConn.Query(sqlQuery)
	defer dbRows.Close()

	if errQuery != nil {
		return nil, errQuery
	}

	jsaResult := jsons.JSONArrayFactory()
	for dbRows.Next() {
		errScan := dbRows.Scan(getDataPointers...)
		if errScan != nil {
			return nil, errScan
		}

		jsoData := jsons.JSONObjectFactory()
		for index, columnItem := range arrColumns {
			pcol := strings.ToLower(strings.TrimSpace(columnItem))
			if strings.HasPrefix(pcol, "txt_") {
				jsoData.PutString(pcol, getData[index].(string))
			} else if strings.HasPrefix(pcol, "int_") {
				jsoData.PutInt(pcol, int(getData[index].(int64)))
			} else if strings.HasPrefix(pcol, "dbl_") {
				jsoData.PutDouble(pcol, getData[index].(float64))
			} else if strings.HasPrefix(pcol, "bln_") {
				jsoData.PutBool(pcol, getData[index].(bool))
			} else if strings.HasPrefix(pcol, "jsa_") {
				jsaColumn, errColumn := jsons.JSONArrayFromString(string(getData[index].([]uint8)))
				if errColumn != nil {
					jsoData.PutArray(pcol, jsons.JSONArrayFactory())
				} else {
					jsoData.PutArray(pcol, jsaColumn)
				}
			} else if strings.HasPrefix(pcol, "jso_") {
				jsoColumn, errColumn := jsons.JSONObjectFromString(string(getData[index].([]uint8)))
				if errColumn != nil {
					jsoData.PutObject(pcol, jsons.JSONObjectFactory())
				} else {
					jsoData.PutObject(pcol, jsoColumn)
				}
			} else if strings.HasPrefix(pcol, "lst_") {
				jsoColumn, errColumn := jsons.JSONObjectFromString(string(getData[index].([]uint8)))
				if errColumn != nil {
					jsoData.PutObject(pcol, jsons.JSONObjectFactory())
				} else {
					jsoData.PutObject(pcol, jsoColumn)
				}
			}
		}
		jsaResult.PutObject(jsoData)
	}
	return jsaResult, nil

}
