package database

import (
	"fmt"

	"github.com/SERV4BIZ/gfp/jsons"
	"github.com/SERV4BIZ/hscale/config/locals"
)

// Listing is list all database
func Listing(jsoCmd *jsons.JSONObject) *jsons.JSONObject {
	jsoResult := jsons.JSONObjectFactory()
	jsoResult.PutInt("status", 0)

	jsaListing, err := locals.ListDatabase()
	if err != nil {
		jsoResult.PutString("txt_msg", fmt.Sprint("Can not listing database [ ", err, " ]"))
		return jsoResult
	}

	jsoData := jsons.JSONObjectFactory()
	jsoData.PutInt("int_length", jsaListing.Length())
	jsoData.PutArray("jsa_listing", jsaListing)
	jsoResult.PutObject("jso_data", jsoData)
	jsoResult.PutInt("status", 1)
	return jsoResult
}
