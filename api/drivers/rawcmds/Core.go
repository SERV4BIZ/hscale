package rawcmds

import "database/sql"

// ConnDriver is interface for connection raw function
type ConnDriver interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Query(query string, args ...interface{}) (*sql.Rows, error)
}
