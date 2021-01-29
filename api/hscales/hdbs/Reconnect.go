package hdbs

// Reconnect is re connection again to database in host and return boolean.
func Reconnect(dataNodeItem *DataNode) {
	if !IsConnect(dataNodeItem) {
		if dataNodeItem.DBConn != nil {
			dataNodeItem.DBConn.Close()
			dataNodeItem.DBConn = nil
		}
		Connect(dataNodeItem)
	}
}
