package nmon2series

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	influxdb "github.com/influxdb/influxdb/client"
)

const timePattern = "15:04:05,02-Jan-2006"

type Section struct {
	Header []string
	Body   [][]string
}

type Nmon struct {
	Sections  map[string]*Section
	Messages  map[string]string
	Snapshots map[string]time.Time
	Hostname  string
}

func NewNmon(data string) (*Nmon, error) {
	data = strings.Replace(data, "%", "Percent", -1)
	nmon := &Nmon{}

	nmon.Sections = nmon.getSections(data)
	if len(nmon.Sections) < 1 {
		return nil, errors.New("No valid data recieved")
	}
	nmon.Hostname = nmon.getHostname()
	nmon.Snapshots = nmon.convertSnapshots()
	nmon.convertTop()
	nmon.Messages = nmon.convertMessages()

	return nmon, nil
}

func (nmon Nmon) GetAsSeries(prefix string, ignoreText bool) (series []*influxdb.Series) {
	if prefix == "" {
		prefix = nmon.Hostname
	} else {
		prefix += "." + nmon.Hostname
	}

	series = append(series, nmon.getSectionsAsSeries(prefix)...)
	if !ignoreText {
		series = append(series, nmon.getMessagesAsSeries(prefix)...)
	}
	return
}

func (nmon Nmon) getSectionsAsSeries(prefix string) (series []*influxdb.Series) {
	for name, section := range nmon.Sections {
		for i, field := range section.Header {
			if i > 0 {
				out := &influxdb.Series{
					Name:    strings.Join([]string{prefix, name, field}, "."),
					Columns: []string{"time", "value"},
				}
				for _, row := range section.Body {
					t := nmon.Snapshots[row[0]].Unix() * 1000
					value, err := strconv.ParseFloat(row[i], 64)
					if err == nil {
						out.Points = append(out.Points, []interface{}{t, value})
					}
				}
				if len(out.Points) > 0 {
					series = append(series, out)
				}
			}
		}
	}
	return
}

func (nmon Nmon) getMessagesAsSeries(prefix string) (series []*influxdb.Series) {
	t := time.Now()
	if len(nmon.Snapshots) > 0 {
		for _, snapshot := range nmon.Snapshots {
			if snapshot.Before(t) {
				t = snapshot
			}
		}
	}
	timestamp := t.Unix() * 1000

	for name, message := range nmon.Messages {
		out := &influxdb.Series{
			Name:    strings.Join([]string{prefix, "MESSAGES", name}, "."),
			Columns: []string{"time", "value"},
		}
		out.Points = append(out.Points, []interface{}{timestamp, message})
		series = append(series, out)
	}
	return
}

func (nmon Nmon) convertTop() {
	nmon.fixTop()

	top, ok := nmon.Sections["TOP"]
	if ok {
		var pidPos int
		var commandPos int
		for i, column := range top.Header {
			switch column {
			case "+PID":
				pidPos = i
			case "Command":
				commandPos = i
			}
		}

		header := remove(top.Header, pidPos, commandPos)

		for _, row := range top.Body {
			name := "TOP." + row[commandPos]
			_, ok := nmon.Sections[name]

			if !ok {
				section := &Section{
					Header: header,
				}
				nmon.Sections[name] = section
			}
			nmon.Sections[name].Body = append(nmon.Sections[name].Body, remove(row, pidPos, commandPos))
		}
	}
	delete(nmon.Sections, "TOP")
}

func remove(s []string, items ...int) []string {
	out := s
	sort.Sort(sort.Reverse(sort.IntSlice(items)))
	for _, item := range items {
		tmp := append(out[:item], out[item+1:]...)
		out = tmp
	}
	return out
}

func (nmon Nmon) fixTop() {
	top, ok := nmon.Sections["TOP"]
	if ok && top.Header[0] == "PercentCPU Utilisation" && len(top.Body) > 0 {
		nmon.Sections["TOP"].Header = nmon.Sections["TOP"].Body[0]
		nmon.Sections["TOP"].Body = nmon.Sections["TOP"].Body[1:]
	}
}

func (nmon Nmon) convertSnapshots() map[string]time.Time {
	snapshots := make(map[string]time.Time)
	for _, row := range nmon.Sections["ZZZZ"].Body {
		if len(row) > 1 {
			t, err := time.Parse(timePattern, row[1])
			if err == nil {
				snapshots[row[0]] = t
			}
		}
	}
	delete(nmon.Sections, "ZZZZ")
	return snapshots
}

func (nmon Nmon) getHostname() string {
	var hostname string
	for _, row := range nmon.Sections["AAA"].Body {
		if len(row) > 1 && row[0] == "host" {
			hostname = row[1]
		}
	}
	return hostname
}

func (nmon Nmon) convertMessages() map[string]string {
	messages := make(map[string]string)
	for name, section := range nmon.Sections {
		switch name {
		case "AAA":
			for _, row := range section.Body {
				msgname := name
				messages[msgname] += strings.Join(row, ": ") + "\n"
			}
			delete(nmon.Sections, name)
		case "BBBP":
			for _, row := range section.Body {
				if len(row) > 2 {
					msgname := name + "_" + row[1]
					messages[msgname] += row[2] + "\n"
				}
			}
			delete(nmon.Sections, name)
		}
	}
	return messages
}

func (nmon Nmon) getSections(data string) map[string]*Section {
	sec := make(map[string]*Section)
	lines := strings.Split(data, "\n")
	for _, line := range lines {
		name, row, err := nmon.readLine(line)

		if err != nil {
			continue
		}

		_, ok := sec[name]

		if !ok {
			header, body := nmon.getHeader(name, row)
			section := &Section{
				Header: header,
			}
			sec[name] = section
			if body != nil {
				sec[name].Body = append(sec[name].Body, body)
			}
		} else {
			sec[name].Body = append(sec[name].Body, row)
		}
	}
	return sec
}

func (nmon Nmon) getHeader(name string, row []string) (header []string, body []string) {
	switch name {
	case "AAA":
		header = append(header, "key", "value")
		body = row
	case "BBBP":
		header = append(header, "line", "source", "value")
		body = row
	case "ZZZZ":
		header = append(header, "snapshot", "time")
		body = row
	default:
		header = row
	}
	return
}

func (nmon Nmon) readLine(line string) (name string, cells []string, err error) {
	err = nil
	s := strings.SplitN(line, ",", 2)
	if len(s) < 2 {
		err = errors.New("Empty line")
		return
	}
	name = s[0]
	row := s[1]
	switch name {
	case "AAA", "ZZZZ":
		cells = strings.SplitN(row, ",", 2)
	case "BBBP":
		cells = strings.SplitN(row, ",", 3)
	default:
		cells = strings.Split(row, ",")
	}
	return
}
