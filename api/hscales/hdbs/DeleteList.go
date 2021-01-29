package hdbs

import (
	"errors"
	"fmt"

	"github.com/SERV4BIZ/gfp/jsons"
)

// DeleteList is delete item in list column
func (me *HDB) DeleteList(txtTable string, txtKeyname string, txtHeadColumn string, txtItemKey string) error {
	jsoRow, errRow := me.GetRow(txtTable, []string{txtHeadColumn}, txtKeyname)
	if errRow != nil {
		return errors.New(fmt.Sprint("Can not get row in ", txtTable, " from key ", txtKeyname, " [ ", errRow, " ]"))
	}

	txtNodeTable := "nodes"
	jsoHeadData := jsoRow.GetObject(txtHeadColumn)
	txtHeadTable := jsoHeadData.GetString("txt_table")

	// Get item data from item key
	jsoItemData, errItemData := me.GetRow(txtHeadTable, []string{}, txtItemKey)
	if errItemData != nil {
		return errors.New(fmt.Sprint("Can not get row data in ", txtHeadTable, " from key ", txtItemKey, " [ ", errItemData, " ]"))
	}

	txtCurrentNodeKey := ""
	var jsoCurrentNodeData *jsons.JSONObject

	blnFound := false
	jsaNewBelong := jsons.JSONArrayFactory()
	jsaBelong := jsoItemData.GetArray("jsa_belong")
	for i := 0; i < jsaBelong.Length(); i++ {
		txtNodeKey := jsaBelong.GetString(i)
		jsoNodeData, errNodeData := me.GetRow(txtNodeTable, []string{}, txtNodeKey)
		if errNodeData != nil {
			return errors.New(fmt.Sprint("Can not get row data in ", txtNodeTable, " from key ", txtNodeKey, " [ ", errNodeData, " ]"))
		}

		if jsoNodeData.GetString("txt_owner_table") == txtTable && jsoNodeData.GetString("txt_owner_keyname") == txtKeyname {
			txtCurrentNodeKey = txtNodeKey
			jsoCurrentNodeData = jsoNodeData
			blnFound = true
		} else {
			jsaNewBelong.PutString(txtNodeKey)
		}
	}

	if !blnFound {
		return errors.New(fmt.Sprint("Not found key ", txtItemKey, " in ", txtHeadColumn, " on table ", txtTable, " keyname row ", txtKeyname))
	}

	// Update node data
	jsoCurrentNodeData.PutInt("int_length", jsoCurrentNodeData.GetInt("int_length")-1)
	jsaNewList := jsons.JSONArrayFactory()
	jsaListing := jsoCurrentNodeData.GetArray("jsa_listing")
	for i := 0; i < jsaListing.Length(); i++ {
		if jsaListing.GetString(i) != txtItemKey {
			jsaNewList.PutString(jsaListing.GetString(i))
		}
	}
	jsoCurrentNodeData.PutArray("jsa_listing", jsaNewList)

	errPutCurrentNode := me.PutRow(txtNodeTable, txtCurrentNodeKey, jsoCurrentNodeData)
	if errPutCurrentNode != nil {
		return errors.New(fmt.Sprint("Can not put node data in ", txtNodeTable, " from key ", txtCurrentNodeKey, " [ ", errPutCurrentNode, " ]"))
	}

	// Update head data
	jsoHeadData.PutInt("int_length", jsoHeadData.GetInt("int_length")-1)
	jsoRow.PutObject(txtHeadColumn, jsoHeadData)
	errPutRow := me.PutRow(txtTable, txtKeyname, jsoRow)
	if errPutRow != nil {
		return errors.New(fmt.Sprint("Can not put row data in ", txtTable, " from key ", txtKeyname, " [ ", errPutRow, " ]"))
	}

	// Update item data belong
	jsoItemData.PutArray("jsa_belong", jsaNewBelong)
	errPutItemData := me.PutRow(txtHeadTable, txtItemKey, jsoItemData)
	if errPutItemData != nil {
		return errors.New(fmt.Sprint("Can not put row data in ", txtHeadTable, " from key ", txtItemKey, " [ ", errPutItemData, " ]"))
	}

	return nil
}
