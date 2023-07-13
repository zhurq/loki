package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/grafana/loki/pkg/logcli/client"
	"github.com/grafana/loki/pkg/logcli/seriesquery"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("logcli", "A command-line for loki.").Version(version.Print("logcli"))
)

func main() {
	var (
		from, to    string
		src         = &client.DefaultClient{}
		dst         = &client.DefaultClient{}
		seriesQuery = &seriesquery.SeriesQuery{
			Matcher: "{}",
		}
	)

	app.Action(func(c *kingpin.ParseContext) error {
		seriesQuery.Start = mustParse(from)
		seriesQuery.End = mustParse(to)

		return nil
	})

	app.Flag("src-addr", "Server address. Can also be set using LOKI_SRC_ADDR env var.").Default("http://localhost:3100").Envar("LOKI_SRC_ADDR").StringVar(&src.Address)
	app.Flag("dst-addr", "Server address. Can also be set using LOKI_DST_ADDR env var.").Default("http://localhost:3101").Envar("LOKI_DST_ADDR").StringVar(&dst.Address)

	app.Flag("org-id", "adds X-Scope-OrgID to API requests for representing tenant ID. Useful for requesting tenant data when bypassing an auth gateway. Can also be set using LOKI_ORG_ID env var.").Default("").Envar("LOKI_ORG_ID").StringVar(&src.OrgID)
	app.Flag("from", "Start looking for logs at this absolute time (inclusive)").StringVar(&from)
	app.Flag("to", "Stop looking for logs at this absolute time (exclusive)").StringVar(&to)

	kingpin.MustParse(app.Parse(os.Args[1:]))

	var buf bytes.Buffer
	seriesQuery.DoSeries(src, &buf)

	series := strings.Split(buf.String(), "\n")
	fmt.Println("series count: ", len(series))

	dst.OrgID = src.OrgID

	/*
		duration := seriesQuery.End.Sub(seriesQuery.Start)
		if duration > 7 * 24 * time.Hour {
			seriesQuery.Start
		}
	*/

	for _, matcher := range series {
		srcResp, err := src.IndexStats(matcher, seriesQuery.Start, seriesQuery.End, true)
		if err != nil {
			log.Fatalf("Error doing request: %+v", err)
			break
		}

		dstResp, err := dst.IndexStats(matcher, seriesQuery.Start, seriesQuery.End, true)
		if err != nil {
			log.Fatalf("Error doing request: %+v", err)
			break
		}

		if !reflect.DeepEqual(srcResp, dstResp) {
			fmt.Printf("mismatch found for series: %v\n", matcher)
			fmt.Println("response from src:")
			fmt.Println(srcResp)
			fmt.Println("response from dst:")
			fmt.Println(dstResp)
		} else {
			fmt.Printf("verified series: %v\n", matcher)
		}

		time.Sleep(time.Second)
	}
}

func mustParse(t string) time.Time {
	ret, err := time.Parse(time.RFC3339Nano, t)

	if err != nil {
		log.Fatalf("Unable to parse time %v", err)
	}

	return ret
}
