package trans

import (
	"errors"
	"fmt"

	"github.com/SERV4BIZ/gfp/jsons"
)

// PutToList is put data in database from node and shard id and keyname
func (me *HDBTx) PutToList(txtTable string, txtKeyname string, txtHeadColumn string, txtItemKey string, jsoItemData *jsons.JSONObject) error {
	jsoRow, errRow := me.GetRow(txtTable, []string{txtHeadColumn}, txtKeyname)
	if errRow != nil {
		return errors.New(fmt.Sprint("Can not get row in ", txtTable, " from key ", txtKeyname, " [ ", errRow, " ]"))
	}

	jsoHeadData := jsoRow.GetObject(txtHeadColumn)
	txtHeadTable := jsoHeadData.GetString("txt_table")

	errAdd := me.AddRow(txtHeadTable, txtItemKey, jsoItemData)
	if errAdd != nil {
		return errors.New(fmt.Sprint("Can not add row to ", txtHeadTable, " [ ", errAdd, " ] "))
	}

	errPut := me.PutList(txtTable, txtKeyname, txtHeadColumn, txtItemKey)
	if errPut != nil {
		return errors.New(fmt.Sprint("Can not put list to ", txtTable, " in column ", txtHeadColumn, " [ ", errAdd, " ] "))
	}
	return nil
}
