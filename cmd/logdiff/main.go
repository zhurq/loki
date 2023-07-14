package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/grafana/loki/pkg/logcli/client"
	"github.com/grafana/loki/pkg/logcli/seriesquery"
	"github.com/prometheus/common/version"
	"github.com/r3labs/diff/v3"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	app = kingpin.New("logcli", "A command-line diff tool for loki.").Version(version.Print("logdiff"))
)

func main() {
	var (
		src = &client.DefaultClient{}
		dst = &client.DefaultClient{}

		fromStr, toStr string
		from, to       time.Time

		limit int
	)

	app.Action(func(c *kingpin.ParseContext) error {
		from = mustParse(fromStr)
		to = mustParse(toStr)

		return nil
	})

	app.Flag("src-addr", "Source server address. Can also be set using LOKI_SRC_ADDR env var.").Default("http://localhost:3100").Envar("LOKI_SRC_ADDR").StringVar(&src.Address)
	app.Flag("dst-addr", "Destination server address. Can also be set using LOKI_DST_ADDR env var.").Default("http://localhost:3101").Envar("LOKI_DST_ADDR").StringVar(&dst.Address)
	app.Flag("org-id", "adds X-Scope-OrgID to API requests for representing tenant. Can also be set using LOKI_ORG_ID env var.").Default("").Envar("LOKI_ORG_ID").StringVar(&src.OrgID)
	app.Flag("from", "Start looking for logs at this absolute time (inclusive)").StringVar(&fromStr)
	app.Flag("to", "Stop looking for logs at this absolute time (exclusive)").StringVar(&toStr)
	app.Flag("limit", "upper limit on the number of series to check for diff. Series are randomly picked if the series count is higher than the limit.").Default("1000").IntVar(&limit)

	kingpin.MustParse(app.Parse(os.Args[1:]))
	dst.OrgID = src.OrgID

	var (
		buf         bytes.Buffer
		seriesQuery = &seriesquery.SeriesQuery{
			Matcher: "{}",
			Start:   from,
			End:     to,
		}
	)

	// fetch all series for the given time range
	seriesQuery.DoSeries(src, &buf)
	seriesList := strings.Split(buf.String(), "\n")
	fmt.Println("series count: ", len(seriesList))

	if len(seriesList) > limit {
		for i := 0; i < limit; i++ {
			pick := rand.Intn(len(seriesList) - i)
			seriesList[i], seriesList[pick+i] = seriesList[pick+i], seriesList[i]
		}

		seriesList = seriesList[:limit]
	}

	for _, series := range seriesList {
		series = strings.TrimSpace(series)
		if len(series) < 2 {
			continue
		}

		if series[0] != '{' || series[len(series)-1] != '}' {
			fmt.Println("skipping series, invalid matchers: ", series)
			continue
		}

		srcResp, err := src.IndexStats(series, from, to, true)
		if err != nil {
			log.Fatalf("Error doing request: %+v", err)
			continue
		}

		dstResp, err := dst.IndexStats(series, from, to, true)
		if err != nil {
			log.Fatalf("Error doing request: %+v", err)
			continue
		}

		if !reflect.DeepEqual(srcResp, dstResp) {
			fmt.Printf("mismatch found for series: %v\n", series)
			changelog, _ := diff.Diff(srcResp, dstResp)
			fmt.Printf("%#v\n", changelog)
		} else {
			fmt.Printf("verified series: %v\n", series)
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
