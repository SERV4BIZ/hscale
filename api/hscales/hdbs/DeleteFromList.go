package hdbs

import (
	"errors"
	"fmt"
)

// DeleteFromList is delete item in list column and origin data
func (me *HDB) DeleteFromList(txtTable string, txtKeyname string, txtHeadColumn string, txtItemKey string) error {
	jsoRow, errRow := me.GetRow(txtTable, []string{txtHeadColumn}, txtKeyname)
	if errRow != nil {
		return errors.New(fmt.Sprint("Can not get row in ", txtTable, " from key ", txtKeyname, " [ ", errRow, " ]"))
	}

	jsoHeadData := jsoRow.GetObject(txtHeadColumn)
	txtHeadTable := jsoHeadData.GetString("txt_table")

	errDel := me.DeleteList(txtTable, txtKeyname, txtHeadColumn, txtItemKey)
	if errDel != nil {
		return errors.New(fmt.Sprint("Can not delete list from ", txtTable, " in ", txtHeadColumn, " [ ", errDel, " ] "))
	}

	errDel = me.DeleteRow(txtHeadTable, txtItemKey)
	if errDel != nil {
		return errors.New(fmt.Sprint("Can not delete row in ", txtHeadTable, " [ ", errDel, " ] "))
	}

	return nil
}
