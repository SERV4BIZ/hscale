package drivers

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SERV4BIZ/escondb"
	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/api/utility"
)

// InsertRow is get data by sql connection
func InsertRow(dbConn *escondb.ESCONTX, sqlInsert string, txtTable string, txtKeyname string, jsoData *jsons.JSONObject) error {
	if jsoData.Length() <= 0 {
		return errors.New("Data value is empty")
	}

	dblStamp := float64(time.Now().Unix())
	jsoData.PutString("txt_keyname", txtKeyname)
	jsoData.PutArray("jsa_belong", jsons.JSONArrayFactory())
	jsoData.PutDouble("dbl_stamp", dblStamp)
	jsoData.PutDouble("dbl_modify", dblStamp)

	arrColumns := jsoData.GetKeys()
	txtColumns := ""
	txtValues := ""
	for _, columnItem := range arrColumns {
		pcol := strings.ToLower(strings.TrimSpace(columnItem))
		txtColumns = fmt.Sprint(txtColumns, columnItem, ",")

		if strings.HasPrefix(pcol, "txt_") {
			txtValues = fmt.Sprint(txtValues, "'", utility.AddQuote(jsoData.GetString(columnItem)), "'", ",")
		} else if strings.HasPrefix(pcol, "int_") {
			txtValues = fmt.Sprint(txtValues, jsoData.GetInt(columnItem), ",")
		} else if strings.HasPrefix(pcol, "dbl_") {
			txtValues = fmt.Sprint(txtValues, jsoData.GetDouble(columnItem), ",")
		} else if strings.HasPrefix(pcol, "bln_") {
			txtValues = fmt.Sprint(txtValues, jsoData.GetBool(columnItem), ",")
		} else if strings.HasPrefix(pcol, "jsa_") {
			txtValues = fmt.Sprint(txtValues, "'", utility.AddQuote(jsoData.GetArray(columnItem).ToString()), "'", ",")
		} else if strings.HasPrefix(pcol, "jso_") {
			txtValues = fmt.Sprint(txtValues, "'", utility.AddQuote(jsoData.GetObject(columnItem).ToString()), "'", ",")
		} else if strings.HasPrefix(pcol, "lst_") {
			txtValues = fmt.Sprint(txtValues, "'", utility.AddQuote(jsoData.GetObject(columnItem).ToString()), "'", ",")
		}
	}
	txtColumns = strings.TrimSpace(strings.Trim(txtColumns, ","))
	txtValues = strings.TrimSpace(strings.Trim(txtValues, ","))

	sqlQuery := sqlInsert
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{columns}", txtColumns)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{values}", txtValues)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{keyname}", utility.AddQuote(txtKeyname))
	_, errExec := dbConn.Exec(sqlQuery)
	if errExec != nil {
		return errExec
	}
	return nil
}
