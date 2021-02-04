package rawcmds

import (
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/jsons"
)

// GetAll is get all data with conditions by sql connection
func GetAll(dbConn *escondb.ESCONTX, sqlFind string, txtTable string, arrColumns []string, intLimit int) (*jsons.JSONArray, error) {
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

	if intLimit >= 0 {
		sqlQuery = strings.ReplaceAll(sqlQuery, "{limit}", fmt.Sprint(intLimit))
	}

	dbRows, errRows := dbConn.Query(sqlQuery)
	if errRows != nil {
		return nil, errRows
	}
	defer dbRows.Close()

	jsaListing := jsons.JSONArrayFactory()
	for dbRows.Next() {
		errGet := dbRows.Scan(getDataPointers...)
		if errGet != nil {
			return nil, errGet
		}

		jsoItem := jsons.JSONObjectFactory()
		for index, columnItem := range arrColumns {
			pcol := strings.ToLower(strings.TrimSpace(columnItem))
			if strings.HasPrefix(pcol, "txt_") {
				jsoItem.PutString(pcol, getData[index].(string))
			} else if strings.HasPrefix(pcol, "int_") {
				jsoItem.PutInt(pcol, int(getData[index].(int64)))
			} else if strings.HasPrefix(pcol, "dbl_") {
				jsoItem.PutDouble(pcol, getData[index].(float64))
			} else if strings.HasPrefix(pcol, "bln_") {
				jsoItem.PutBool(pcol, getData[index].(bool))
			} else if strings.HasPrefix(pcol, "jsa_") {
				jsaColumn, errColumn := jsons.JSONArrayFromString(string(getData[index].([]uint8)))
				if errColumn != nil {
					jsoItem.PutArray(pcol, jsons.JSONArrayFactory())
				} else {
					jsoItem.PutArray(pcol, jsaColumn)
				}
			} else if strings.HasPrefix(pcol, "jso_") {
				jsoColumn, errColumn := jsons.JSONObjectFromString(string(getData[index].([]uint8)))
				if errColumn != nil {
					jsoItem.PutObject(pcol, jsons.JSONObjectFactory())
				} else {
					jsoItem.PutObject(pcol, jsoColumn)
				}
			} else if strings.HasPrefix(pcol, "lst_") {
				jsoColumn, errColumn := jsons.JSONObjectFromString(string(getData[index].([]uint8)))
				if errColumn != nil {
					jsoItem.PutObject(pcol, jsons.JSONObjectFactory())
				} else {
					jsoItem.PutObject(pcol, jsoColumn)
				}
			}
		}
		jsaListing.PutObject(jsoItem)
	}

	return jsaListing, nil
}
