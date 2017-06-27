package main

import (
	"flag"
	"log"
	"strings"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

func main() {
	var (
		table     string
		zk        string
		threshold float64
	)

	flag.StringVar(&table, "table", "meta", "Table to compact")
	flag.StringVar(&zk, "zk", "localhost", "Zookeeper Quorum")
	flag.Float64Var(&threshold, "threshold", 0.5, "Threshold to compact a region")
	flag.Parse()

	log.Printf("Compacting table: %s", table)

	client := gohbase.NewAdminClient(zk)

	s, err := client.ClusterStatus()
	if err != nil {
		log.Fatalf("Failed to get status: %+v", err)
	}

	servers := s.GetLiveServers()
	for _, server := range servers {
		name := server.GetServer().GetHostName()

		log.Printf("Compacting on: %s", name)

		load := server.GetServerLoad()
		rloads := load.GetRegionLoads()

		for _, rload := range rloads {
			rname := string(rload.RegionSpecifier.GetValue())
			if strings.Contains(rname, table+",") && rload.GetDataLocality() <= float32(threshold) {
				cr := hrpc.NewCompactRegion(rname, []byte{}, false) //TODO:: add family, major options
				if err := client.CompactRegion(cr); err != nil {
					log.Fatalf("Failed to compact region (%s): %+v", rname, err)
				}
			}
		}
	}
}
