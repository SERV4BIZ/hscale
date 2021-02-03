package hdbs

import (
	"errors"
	"fmt"
	"strings"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/gfp/uuid"
)

// NewListNode is create a new list node of column list
func (me *HDBTX) NewListNode(txtTable string, txtKeyname string, txtHeadColumn string) (*jsons.JSONObject, error) {
	jsoRow, errRow := me.GetRow(txtTable, []string{txtHeadColumn}, txtKeyname)
	if errRow != nil {
		return nil, errors.New(fmt.Sprint("Can not get row in ", txtTable, " from key ", txtKeyname, " [ ", errRow, " ]"))
	}

	txtNodeTable := "nodes"
	jsoHeadData := jsoRow.GetObject(txtHeadColumn)
	txtHeadTable := jsoHeadData.GetString("txt_table")

	// Create a node
	txtNewNodeKeyname, errNewNodeKeyname := uuid.NewV4()
	if errNewNodeKeyname != nil {
		return nil, errors.New(fmt.Sprint("Can not generate uuid [ ", errNewNodeKeyname, " ]"))
	}

	jsoNewNodeData := jsons.JSONObjectFactory()
	jsoNewNodeData.PutString("txt_owner_table", txtTable)
	jsoNewNodeData.PutString("txt_owner_keyname", txtKeyname)
	jsoNewNodeData.PutString("txt_table", txtHeadTable)
	jsoNewNodeData.PutInt("int_length", 0)
	jsoNewNodeData.PutString("txt_back_node", "#")
	jsoNewNodeData.PutString("txt_next_node", "#")
	jsoNewNodeData.PutArray("jsa_listing", jsons.JSONArrayFactory())

	// Create a new first node if not have
	if strings.TrimSpace(jsoHeadData.GetString("txt_first_node")) == "#" && strings.TrimSpace(jsoHeadData.GetString("txt_last_node")) == "#" {
		jsoHeadData.PutString("txt_first_node", txtNewNodeKeyname)
		jsoHeadData.PutString("txt_last_node", txtNewNodeKeyname)

		errPutNode := me.PutRow(txtNodeTable, txtNewNodeKeyname, jsoNewNodeData)
		if errPutNode != nil {
			return nil, errors.New(fmt.Sprint("Can not put node data in ", txtNodeTable, " from key ", txtNewNodeKeyname, " [ ", errPutNode, " ]"))
		}

		jsoRow.PutObject(txtHeadColumn, jsoHeadData)
		errPutRow := me.PutRow(txtTable, txtKeyname, jsoRow)
		if errPutRow != nil {
			return nil, errors.New(fmt.Sprint("Can not put row data in ", txtTable, " from key ", txtKeyname, " [ ", errPutRow, " ]"))
		}

		return jsoNewNodeData, nil
	}

	// If not first node and then insert it
	txtLastNodeKey := jsoHeadData.GetString("txt_last_node")

	jsoLastNodeData, errLastNodeData := me.GetRow(txtNodeTable, []string{}, txtLastNodeKey)
	if errLastNodeData != nil {
		return nil, errors.New(fmt.Sprint("Can not get row in ", txtNodeTable, " from key ", txtLastNodeKey, " [ ", errLastNodeData, " ]"))
	}

	// New node pointer back to last before
	jsoNewNodeData.PutString("txt_back_node", txtLastNodeKey)
	jsoNewNodeData.PutString("txt_next_node", "#")

	errPutNewNode := me.PutRow(txtNodeTable, txtNewNodeKeyname, jsoNewNodeData)
	if errPutNewNode != nil {
		return nil, errors.New(fmt.Sprint("Can not put node data in ", txtNodeTable, " from key ", txtNewNodeKeyname, " [ ", errPutNewNode, " ]"))
	}

	// Last node pointer next to new node
	jsoLastNodeData.PutString("txt_next_node", txtNewNodeKeyname)

	errPutLastNode := me.PutRow(txtNodeTable, txtLastNodeKey, jsoLastNodeData)
	if errPutLastNode != nil {
		return nil, errors.New(fmt.Sprint("Can not put node data in ", txtNodeTable, " from key ", txtLastNodeKey, " [ ", errPutLastNode, " ]"))
	}

	// Head column pointer last to new node
	jsoHeadData.PutString("txt_last_node", txtNewNodeKeyname)

	jsoRow.PutObject(txtHeadColumn, jsoHeadData)
	errPutRow := me.PutRow(txtTable, txtKeyname, jsoRow)
	if errPutRow != nil {
		return nil, errors.New(fmt.Sprint("Can not put row data in ", txtTable, " from key ", txtKeyname, " [ ", errPutRow, " ]"))
	}

	return jsoNewNodeData, nil
}
