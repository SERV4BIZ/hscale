package hdbs

import (
	"github.com/SERV4BIZ/gfp/jsons"
)

// New is begin HScaleDB object
func New(jsoConfigHost *jsons.JSONObject) (*HDB, error) {
	return Factory(jsoConfigHost)
}
