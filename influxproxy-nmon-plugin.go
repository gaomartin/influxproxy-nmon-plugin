package main

import (
	"fmt"

	"github.com/influxdb/influxdb-go"
	"github.com/influxproxy/influxproxy/plugin"
)

type Functions struct{}

func (f Functions) Describe() plugin.Description {
	d := plugin.Description{
		Description: "This plugin takes nmon reports and pushes them to the given influxdb",
		Author:      "github.com/sontags",
		Version:     "0.1.0",
		Arguments: []plugin.Argument{
			{
				Name:        "prefix",
				Description: "Prefix of the series, will be separated with a '.' if given",
				Optional:    true,
			},
		},
	}
	return d
}

func (f Functions) Run(in plugin.Request) plugin.Response {
	// TODO: implement...
	var series []*influxdb.Series
	return plugin.Response{
		Series: series,
		Error:  "",
	}
}

func main() {
	f := Functions{}
	p, err := plugin.NewPlugin()
	if err != nil {
		fmt.Println(err)
	} else {
		p.Run(f)
	}
}
