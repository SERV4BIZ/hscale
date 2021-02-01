package hdbs

// IsConnect is check connection to database in host and return boolean.
func IsConnect(dataNodeItem *DataNode) bool {
	if dataNodeItem.DBConn != nil {
		return dataNodeItem.DBConn.Ping() == nil
	}
	return false
}
