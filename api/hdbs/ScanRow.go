package hdbs

import (
	"fmt"
	"strings"
	"time"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/gfp/uuid"
	"github.com/SERV4BIZ/hscale/api/drivers/rawcmds"
	"github.com/SERV4BIZ/hscale/api/utility"
)

// ScanRowWorker is thread loop scan all data and add to listing for scan function
func ScanRowWorker(hdbItem *HDB, scanItem *DataScan, txtTable string, arrColumns []string) {
	columns := arrColumns
	dblExpire := float64(60 * 3)

	hdbItem.MutexMapDataNode.RLock()
	jsaNodeKey := jsons.JSONArrayFactory()
	for key := range hdbItem.MapDataNode {
		jsaNodeKey.PutString(key)
	}
	hdbItem.MutexMapDataNode.RUnlock()

	for jsaNodeKey.Length() > 0 {
		<-time.After(time.Millisecond)

		index := utility.RandomIntn(jsaNodeKey.Length())
		nodeName := jsaNodeKey.GetString(index)
		jsaNodeKey.Remove(index)

		hdbItem.MutexMapDataNode.RLock()
		dataNodeItem := hdbItem.MapDataNode[nodeName]
		hdbItem.MutexMapDataNode.RUnlock()

		Reconnect(dataNodeItem)
		dataNodeItem.RLock()
		sqlListing := dataNodeItem.JSOSQLDriver.GetString("listing")
		sqlListColumn := dataNodeItem.JSOSQLDriver.GetString("listing_column")
		dataNodeItem.RUnlock()

		if dataNodeItem.DBConn == nil {
			scanItem.IsError = true
			scanItem.ErrorMsg = "Connection is not open"
			break
		}

		var errColumns error
		if len(columns) == 0 {
			columns, errColumns = rawcmds.ListColumns(dataNodeItem.DBConn, sqlListColumn, txtTable)
		}

		if errColumns != nil {
			scanItem.IsError = true
			scanItem.ErrorMsg = fmt.Sprint("Columns is empty [ ", errColumns, " ]")
			break
		}

		intOffset := 0
		intLimit := 100
		for {
			<-time.After(time.Millisecond)

			jsaListing, errListing := rawcmds.Listing(dataNodeItem.DBConn, sqlListing, txtTable, columns, intOffset, intLimit)
			if errListing != nil {
				scanItem.IsError = true
				scanItem.ErrorMsg = fmt.Sprint("Can not listing from table [ ", errListing, " ]")
				break
			}

			if jsaListing.Length() == 0 {
				break
			}

			for i := 0; i < jsaListing.Length(); i++ {
				scanItem.Lock()
				scanItem.JSAList.PutObject(jsaListing.GetObject(i))
				scanItem.Unlock()
			}

			intOffset += intLimit

			// Check expire
			if float64(float64(time.Now().Unix())-scanItem.Read) >= dblExpire {
				scanItem.IsExpire = true
				break
			}
		}

		if scanItem.IsError {
			break
		}

		// Check expire
		if float64(float64(time.Now().Unix())-scanItem.Read) >= dblExpire {
			scanItem.IsExpire = true
			break
		}
	}

	scanItem.Lock()
	scanItem.IsFinish = true
	scanItem.Unlock()
}

// ScanRow is list data in database from node
func (me *HDB) ScanRow(txtTable string, arrColumns []string, txtUUID string) *jsons.JSONObject {
	jsoResult := jsons.JSONObjectFactory()
	me.MutexMapDataScan.RLock()
	scanItem, scanOk := me.MapDataScan[txtUUID]
	me.MutexMapDataScan.RUnlock()

	if scanOk {
		jsaList := jsons.JSONArrayFactory()

		scanItem.Lock()
		scanItem.Read = float64(time.Now().Unix())
		for scanItem.JSAList.Length() > 0 {
			jsaList.PutObject(scanItem.JSAList.GetObject(0))
			scanItem.JSAList.Remove(0)
		}
		scanItem.Unlock()

		jsoResult.PutDouble("dbl_stamp", scanItem.Stamp)
		jsoResult.PutDouble("dbl_read", scanItem.Read)
		jsoResult.PutString("txt_uuid", scanItem.UUID)
		jsoResult.PutBool("bln_error", scanItem.IsError)
		jsoResult.PutString("txt_msg", scanItem.ErrorMsg)
		jsoResult.PutBool("bln_finish", scanItem.IsFinish)
		jsoResult.PutBool("bln_expire", scanItem.IsExpire)
		jsoResult.PutArray("jsa_listing", jsaList)
	} else {
		scanItem = new(DataScan)
		scanItem.HDB = me
		scanItem.Stamp = float64(time.Now().Unix())
		scanItem.Read = scanItem.Stamp
		if strings.TrimSpace(txtUUID) != "" {
			scanItem.UUID = txtUUID
		} else {
			txtNewUUID, err := uuid.NewV4()
			if err == nil {
				scanItem.UUID = txtNewUUID
			} else {
				scanItem.UUID = fmt.Sprint(time.Now().Unix())
			}
		}
		scanItem.IsFinish = false
		scanItem.IsError = false
		scanItem.IsExpire = false
		scanItem.ErrorMsg = ""
		scanItem.JSAList = jsons.JSONArrayFactory()

		me.MutexMapDataScan.RLock()
		me.MapDataScan[txtUUID] = scanItem
		me.MutexMapDataScan.RUnlock()

		jsoResult.PutDouble("dbl_stamp", scanItem.Stamp)
		jsoResult.PutDouble("dbl_read", scanItem.Read)
		jsoResult.PutString("txt_uuid", scanItem.UUID)
		jsoResult.PutBool("bln_error", scanItem.IsError)
		jsoResult.PutString("txt_msg", scanItem.ErrorMsg)
		jsoResult.PutBool("bln_finish", scanItem.IsFinish)
		jsoResult.PutBool("bln_expire", scanItem.IsExpire)
		jsoResult.PutArray("jsa_listing", scanItem.JSAList)

		go ScanRowWorker(me, scanItem, txtTable, arrColumns)
	}

	return jsoResult
}
