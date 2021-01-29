package rawcmds

import (
	"strings"
)

// ListColumns is get all fields or columns of table by sql connection
func ListColumns(dbConn ConnDriver, sqlListColumn string, txtTable string) ([]string, error) {
	sqlQuery := sqlListColumn
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", txtTable)
	dbRows, errRow := dbConn.Query(sqlQuery)
	defer dbRows.Close()

	if errRow != nil {
		return nil, errRow
	}

	arrColumns := make([]string, 0)
	txtColumnName := ""
	for dbRows.Next() {
		errScan := dbRows.Scan(&txtColumnName)
		if errScan != nil {
			return nil, errScan
		}
		arrColumns = append(arrColumns, txtColumnName)
	}
	return arrColumns, nil

}
