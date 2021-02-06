package hscales

import (
	"errors"
	"fmt"
	"strings"
)

// PutList is put data in database from node and shard id and keyname
func (me *HDBTX) PutList(txtTable string, txtKeyname string, txtHeadColumn string, txtItemKey string) error {
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

	jsaBelong := jsoItemData.GetArray("jsa_belong")
	for i := 0; i < jsaBelong.Length(); i++ {
		txtNodeKey := jsaBelong.GetString(i)
		jsoNodeData, errNodeData := me.GetRow(txtNodeTable, []string{}, txtNodeKey)
		if errNodeData != nil {
			return errors.New(fmt.Sprint("Can not get row data in ", txtNodeTable, " from key ", txtNodeKey, " [ ", errNodeData, " ]"))
		}

		if jsoNodeData.GetString("txt_owner_table") == txtTable && jsoNodeData.GetString("txt_owner_keyname") == txtKeyname {
			return errors.New("Already data in list")
		}
	}

	// If not found in list
	// Create a new first node if not have
	if strings.TrimSpace(jsoHeadData.GetString("txt_first_node")) == "#" && strings.TrimSpace(jsoHeadData.GetString("txt_last_node")) == "#" {
		// Create a node
		_, errNewNode := me.NewListNode(txtTable, txtKeyname, txtHeadColumn)
		if errNewNode != nil {
			return errors.New(fmt.Sprint("Can not generate new node in ", txtTable, " from key ", txtKeyname, " [ ", errNewNode, " ]"))
		}

		// Update header info
		jsoRow, errRow = me.GetRow(txtTable, []string{txtHeadColumn}, txtKeyname)
		if errRow != nil {
			return errors.New(fmt.Sprint("Can not get row in ", txtTable, " from key ", txtKeyname, " [ ", errRow, " ]"))
		}

		jsoHeadData = jsoRow.GetObject(txtHeadColumn)
	}

	// Get last node from main row
	txtLastNodeKey := jsoHeadData.GetString("txt_last_node")
	jsoLastNodeData, errLastNodeData := me.GetRow(txtNodeTable, []string{}, txtLastNodeKey)
	if errLastNodeData != nil {
		return errors.New(fmt.Sprint("Can not get row data in ", txtNodeTable, " from key ", txtLastNodeKey, " [ ", errLastNodeData, " ]"))
	}
	jsoLastNodeData.GetArray("jsa_listing").PutString(txtItemKey)
	jsoLastNodeData.PutInt("int_length", jsoLastNodeData.GetInt("int_length")+1)

	errPutLastNode := me.PutRow(txtNodeTable, txtLastNodeKey, jsoLastNodeData)
	if errPutLastNode != nil {
		return errors.New(fmt.Sprint("Can not put node data in ", txtNodeTable, " from key ", txtLastNodeKey, " [ ", errPutLastNode, " ]"))
	}

	// Update head data
	jsoHeadData.PutInt("int_length", jsoHeadData.GetInt("int_length")+1)
	jsoRow.PutObject(txtHeadColumn, jsoHeadData)
	errPutRow := me.PutRow(txtTable, txtKeyname, jsoRow)
	if errPutRow != nil {
		return errors.New(fmt.Sprint("Can not put row data in ", txtTable, " from key ", txtKeyname, " [ ", errPutRow, " ]"))
	}

	// Update item data belong
	jsaBelong.PutString(txtLastNodeKey)
	jsoItemData.PutArray("jsa_belong", jsaBelong)
	errPutItemData := me.PutRow(txtHeadTable, txtItemKey, jsoItemData)
	if errPutItemData != nil {
		return errors.New(fmt.Sprint("Can not put row data in ", txtHeadTable, " from key ", txtItemKey, " [ ", errPutItemData, " ]"))
	}

	// Check if listing in node more than 1000 record then create a new node
	if jsoLastNodeData.GetInt("int_length") >= 1000 {
		// Create a node
		_, errNewNode := me.NewListNode(txtTable, txtKeyname, txtHeadColumn)
		if errNewNode != nil {
			return errors.New(fmt.Sprint("Can not generate new node in ", txtTable, " from key ", txtKeyname, " [ ", errNewNode, " ]"))
		}
	}

	return nil
}
