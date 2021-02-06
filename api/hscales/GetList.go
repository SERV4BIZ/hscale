package hscales

import (
	"errors"
	"fmt"

	"github.com/SERV4BIZ/gfp/jsons"
)

// GetList is put data in database from node and shard id and keyname
func (me *HDBTX) GetList(txtTable string, txtKeyname string, txtHeadColumn string, arrColumns []string, intOffet int, intLimit int) (*jsons.JSONArray, error) {
	jsoRow, errRow := me.GetRow(txtTable, []string{txtHeadColumn}, txtKeyname)
	if errRow != nil {
		return nil, errors.New(fmt.Sprint("Can not get row in ", txtTable, " from key ", txtKeyname, " [ ", errRow, " ]"))
	}

	txtNodeTable := "nodes"
	jsoHeadData := jsoRow.GetObject(txtHeadColumn)
	txtHeadTable := jsoHeadData.GetString("txt_table")

	jsaResult := jsons.JSONArrayFactory()
	intIndex := 0
	intCount := 0

	txtNodeKey := jsoHeadData.GetString("txt_last_node")
	if txtNodeKey == "#" || txtNodeKey == "" {
		return nil, errors.New("Can not get key from last node")
	}

	for {
		// Get last node from main row
		jsoNodeData, errNodeData := me.GetRow(txtNodeTable, []string{"txt_back_node", "jsa_listing"}, txtNodeKey)
		if errNodeData != nil {
			return nil, errors.New(fmt.Sprint("Can not get row data in ", txtNodeTable, " from key ", txtNodeKey, " [ ", errNodeData, " ]"))
		}

		jsaListing := jsoNodeData.GetArray("jsa_listing")
		for i := jsaListing.Length() - 1; i >= 0; i-- {
			if intIndex >= intOffet {
				intCount++
				txtItemKey := jsaListing.GetString(i)

				// Get item data from item key
				jsoItemData, errItemData := me.GetRow(txtHeadTable, arrColumns, txtItemKey)
				if errItemData != nil {
					return nil, errors.New(fmt.Sprint("Can not get row data in ", txtHeadTable, " from key ", txtItemKey, " [ ", errItemData, " ]"))
				}
				jsaResult.PutObject(jsoItemData)

				if intCount >= intLimit && intLimit > 0 {
					return jsaResult, nil
				}
			}
			intIndex++
		}

		// Back node
		txtNodeKey = jsoNodeData.GetString("txt_back_node")
		if txtNodeKey == "#" || txtNodeKey == "" {
			return jsaResult, nil
		}
	}

	return jsaResult, nil
}
