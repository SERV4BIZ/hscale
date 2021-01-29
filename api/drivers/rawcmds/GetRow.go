package rawcmds

import (
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/utilities"
)

// GetRow is get data by sql connection
func GetRow(dbConn ConnDriver, sqlSelect string, txtTable string, arrColumns []string, txtKeyname string) (*jsons.JSONObject, error) {
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

	sqlQuery := sqlSelect
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{columns}", txtColumns)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{keyname}", utilities.AddQuote(txtKeyname))
	errGet := dbConn.QueryRow(sqlQuery).Scan(getDataPointers...)
	if errGet != nil {
		return nil, errGet
	}

	jsoResult := jsons.JSONObjectFactory()
	for index, columnItem := range arrColumns {
		pcol := strings.ToLower(strings.TrimSpace(columnItem))
		if strings.HasPrefix(pcol, "txt_") {
			jsoResult.PutString(pcol, getData[index].(string))
		} else if strings.HasPrefix(pcol, "int_") {
			jsoResult.PutInt(pcol, int(getData[index].(int64)))
		} else if strings.HasPrefix(pcol, "dbl_") {
			jsoResult.PutDouble(pcol, getData[index].(float64))
		} else if strings.HasPrefix(pcol, "bln_") {
			jsoResult.PutBool(pcol, getData[index].(bool))
		} else if strings.HasPrefix(pcol, "jsa_") {
			jsaColumn, errColumn := jsons.JSONArrayFromString(string(getData[index].([]uint8)))
			if errColumn != nil {
				jsoResult.PutArray(pcol, jsons.JSONArrayFactory())
			} else {
				jsoResult.PutArray(pcol, jsaColumn)
			}
		} else if strings.HasPrefix(pcol, "jso_") {
			jsoColumn, errColumn := jsons.JSONObjectFromString(string(getData[index].([]uint8)))
			if errColumn != nil {
				jsoResult.PutObject(pcol, jsons.JSONObjectFactory())
			} else {
				jsoResult.PutObject(pcol, jsoColumn)
			}
		} else if strings.HasPrefix(pcol, "lst_") {
			jsoColumn, errColumn := jsons.JSONObjectFromString(string(getData[index].([]uint8)))
			if errColumn != nil {
				jsoResult.PutObject(pcol, jsons.JSONObjectFactory())
			} else {
				jsoResult.PutObject(pcol, jsoColumn)
			}
		}
	}
	return jsoResult, nil
}
