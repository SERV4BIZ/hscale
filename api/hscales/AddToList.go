package hscales

import "github.com/SERV4BIZ/gfp/jsons"

// AddToList is same PutToList function
func (me *HDBTX) AddToList(txtTable string, txtKeyname string, txtHeadColumn string, txtItemKey string, jsoItemData *jsons.JSONObject) error {
	return me.PutToList(txtTable, txtKeyname, txtHeadColumn, txtItemKey, jsoItemData)
}
