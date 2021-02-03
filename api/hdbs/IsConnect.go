package hdbs

// IsConnect is check connection to database in host and return boolean.
func (me *DataNode) IsConnect() bool {
	if me.DBConn != nil {
		return me.DBConn.Ping() == nil
	}
	return false
}
