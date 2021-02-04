package rawcmds

import (
	"strings"
)

// ListColumns is get all fields or columns of table by sql connection
func ListColumns(dbConn *escondb.ESCONTX, sqlListColumn string, txtTable string) ([]string, error) {
	sqlQuery := sqlListColumn
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	jsaRow, errRow := dbConn.Query(sqlQuery)
	if errRow != nil {
		return nil, errRow
	}

	arrColumns := make([]string, 0)
	for i:=0;i<jsaRow.Length();i++ {
		arrColumns = append(arrColumns, jsaRow.GetObject(i).GetString("COLUMN_NAME"))
	}
	return arrColumns, nil

}
