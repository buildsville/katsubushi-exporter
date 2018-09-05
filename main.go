package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
)

const (
	defaultMetricsInterval = 30
	defaultAddr            = ":9298"
	defaultKatsubushiHost  = "localhost"
	defaultKatsubushiPort  = "11212"
	retryDuration          = 1
)

const rootDoc = `<html>
<head><title>katsubushi Exporter</title></head>
<body>
<h1>katsubushi Exporter</h1>
<p><a href="/metrics">Metrics</a></p>
</body>
</html>
`

var addr = flag.String("listen-address", defaultAddr, "The address to listen on for HTTP requests.")
var metricsInterval = flag.Int("metricsInterval", defaultMetricsInterval, "Interval to scrape katsubushi stats.")
var katsubushiHost = flag.String("katsubushiHost", defaultKatsubushiHost, "target katsubushi host.")
var katsubushiPort = flag.String("katsubushiPort", defaultKatsubushiPort, "target katsubushi port.")

var infoLabels = []string{
	"katsubushi_version",
	"katsubushi_pid",
}

var labels = []string{}

var (
	katsubushiInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "katsubushi_info",
			Help: "Information of katsubushi.",
		},
		infoLabels,
	)

	katsubushiUptime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "katsubushi_uptime",
			Help: "Uptime of katsubushi process.",
		},
		labels,
	)

	katsubushiCurrConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "katsubushi_curr_connections",
			Help: "Current connection.",
		},
		labels,
	)

	katsubushiTotalConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "katsubushi_total_connections",
			Help: "Total connection.",
		},
		labels,
	)

	katsubushiCmdGet = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "katsubushi_get_cmd",
			Help: "Number of GET command.",
		},
		labels,
	)

	katsubushiGetHits = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "katsubushi_get_hits",
			Help: "Number of Get command success.",
		},
		labels,
	)

	katsubushiGetMisses = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "katsubushi_get_misses",
			Help: "Number of Get command miss.",
		},
		labels,
	)
)

func init() {
	prometheus.MustRegister(katsubushiInfo)
	prometheus.MustRegister(katsubushiUptime)
	prometheus.MustRegister(katsubushiCurrConnections)
	prometheus.MustRegister(katsubushiTotalConnections)
	prometheus.MustRegister(katsubushiCmdGet)
	prometheus.MustRegister(katsubushiGetHits)
	prometheus.MustRegister(katsubushiGetMisses)
}

func getKatsubushiStats() (map[string]string, map[string]float64, error) {
	network := "tcp"
	target := fmt.Sprintf("%s:%s", *katsubushiHost, *katsubushiPort)
	conn, err := net.Dial(network, target)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	fmt.Fprint(conn, "STATS\r\n")

	info := map[string]string{}
	stats := map[string]float64{}
	sc := bufio.NewScanner(conn)
	for sc.Scan() {
		s := sc.Text()
		if s == "END" {
			break
		}

		res := strings.Split(s, " ")
		if res[0] == "STAT" {
			if res[1] == "pid" || res[1] == "version" {
				info[res[1]] = res[2]
			} else {
				if f, err := strconv.ParseFloat(res[2], 64); err == nil {
					stats[res[1]] = f
				} else {
					return nil, nil, err
				}
			}
		}
	}
	err = sc.Err()
	return info, stats, err
}

func main() {
	flag.Parse()
	log.Info("start katsubushi exporter")

	go func() {
		for {
			info, stats, err := getKatsubushiStats()
			if err != nil {
				log.Errorln(err)
				time.Sleep(time.Duration(retryDuration) * time.Second)
				continue
			}
			if info["version"] == "" || info["pid"] == "" {
				log.Info("Retry since info(version or pid) is empty")
				time.Sleep(time.Duration(retryDuration) * time.Second)
				continue
			}
			infoLabel := prometheus.Labels{
				"katsubushi_version": info["version"],
				"katsubushi_pid":     info["pid"],
			}
			label := prometheus.Labels{}
			katsubushiInfo.With(infoLabel).Set(float64(1))
			katsubushiUptime.With(label).Set(stats["uptime"])
			katsubushiCurrConnections.With(label).Set(stats["curr_connections"])
			katsubushiTotalConnections.With(label).Set(stats["total_connections"])
			katsubushiCmdGet.With(label).Set(stats["cmd_get"])
			katsubushiGetHits.With(label).Set(stats["get_hits"])
			katsubushiGetMisses.With(label).Set(stats["get_misses"])
			time.Sleep(time.Duration(*metricsInterval) * time.Second)
		}
	}()
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(rootDoc))
	})

	log.Fatal(http.ListenAndServe(*addr, nil))
}
