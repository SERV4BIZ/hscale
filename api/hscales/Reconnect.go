package hscales

// Reconnect is re connection again to database in host and return boolean.
func (me *DataNode) Reconnect() {
	if !me.IsConnect() {
		if me.DBConn != nil {
			me.DBConn.Close()
			me.DBConn = nil
		}
		me.Connect()
	}
}
