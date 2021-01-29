package postgresql

import (
	"database/sql"
	"fmt"

	"github.com/SERV4BIZ/gfp/handler"
	_ "github.com/lib/pq"
)

// Connect is access to database host
func Connect(host string, port int, username string, password string, dbname string) (*sql.DB, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, username, password, dbname)
	conn, errConn := sql.Open("postgres", psqlInfo)
	if !handler.Error(errConn) {
		errPing := conn.Ping()
		if !handler.Error(errPing) {
			return conn, errPing
		}
		conn.Close()
		conn = nil
		return nil, errPing
	}
	return nil, errConn
}
