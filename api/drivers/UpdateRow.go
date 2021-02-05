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

// UpdateRow is get data by sql connection
func UpdateRow(dbConn *escondb.ESCONTX, sqlUpdate string, txtTable string, txtKeyname string, jsoData *jsons.JSONObject) error {
	if jsoData.Length() <= 0 {
		return errors.New("Data value is empty")
	}

	jsoData.PutDouble("dbl_modify", float64(time.Now().Unix()))
	arrColumns := jsoData.GetKeys()
	txtColumns := ""
	for _, columnItem := range arrColumns {
		pcol := strings.ToLower(strings.TrimSpace(columnItem))
		if strings.HasPrefix(pcol, "txt_") {
			txtColumns = fmt.Sprint(txtColumns, columnItem, "='", utility.AddQuote(jsoData.GetString(columnItem)), "'", ",")
		} else if strings.HasPrefix(pcol, "int_") {
			txtColumns = fmt.Sprint(txtColumns, columnItem, "=", jsoData.GetInt(columnItem), ",")
		} else if strings.HasPrefix(pcol, "dbl_") {
			txtColumns = fmt.Sprint(txtColumns, columnItem, "=", jsoData.GetDouble(columnItem), ",")
		} else if strings.HasPrefix(pcol, "bln_") {
			txtColumns = fmt.Sprint(txtColumns, columnItem, "=", jsoData.GetBool(columnItem), ",")
		} else if strings.HasPrefix(pcol, "jsa_") {
			txtColumns = fmt.Sprint(txtColumns, columnItem, "='", utility.AddQuote(jsoData.GetArray(columnItem).ToString()), "'", ",")
		} else if strings.HasPrefix(pcol, "jso_") {
			txtColumns = fmt.Sprint(txtColumns, columnItem, "='", utility.AddQuote(jsoData.GetObject(columnItem).ToString()), "'", ",")
		} else if strings.HasPrefix(pcol, "lst_") {
			txtColumns = fmt.Sprint(txtColumns, columnItem, "='", utility.AddQuote(jsoData.GetObject(columnItem).ToString()), "'", ",")
		}
	}
	txtColumns = strings.TrimSpace(strings.Trim(txtColumns, ","))

	sqlQuery := sqlUpdate
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{values}", txtColumns)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{keyname}", utility.AddQuote(txtKeyname))
	_, errExec := dbConn.Exec(sqlQuery)
	if errExec != nil {
		return errExec
	}
	return nil
}
