package main

import (
	"fmt"

	"github.com/influxproxy/influxproxy-nmon-plugin/nmon2series"
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
				Default:     "",
			},
			{
				Name:        "ignore_text",
				Description: "If any value is provided, the text passages of the nmon report (e.g. AAA and BBBP sections) will not be ignored",
				Optional:    true,
				Default:     "",
			},
		},
	}
	return d
}

func (f Functions) Run(in plugin.Request) plugin.Response {
	ignoreText := false
	if in.Query.Get("ignore_text") != "" {
		ignoreText = true
	}

	nmon, err := nmon2series.NewNmon(in.Body)
	if err != nil {
		return plugin.Response{
			Series: nil,
			Error:  err.Error(),
		}
	}

	series := nmon.GetAsSeries(in.Query.Get("prefix"), ignoreText)

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
