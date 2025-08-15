// Example of using the ping functionality with RegionClient
// This example demonstrates how to enable ping monitoring for a RegionClient
// and expose the metrics via HTTP endpoint for Prometheus scraping.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tsuna/gohbase/region"
)

func main() {
	// Define command-line flags
	var (
		regionServer = flag.String("regionserver", "localhost:16020", "HBase region server address")
		interval     = flag.Duration("interval", 10*time.Second, "Ping interval")
		windowSize   = flag.Int("window", 10, "Number of latest ping measurements to keep")
		httpPort     = flag.String("port", "2112", "HTTP port for Prometheus metrics")
	)
	
	flag.Parse()

	// Start Prometheus metrics HTTP server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		addr := ":" + *httpPort
		log.Printf("Prometheus metrics available at http://localhost:%s/metrics", *httpPort)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatal("Failed to start metrics server:", err)
		}
	}()

	// Create a RegionClient with ping enabled
	client := region.NewClient(
		*regionServer,
		region.RegionClient,
		region.WithPingInterval(*interval),
		region.WithPingLatencyWindow(*windowSize),
	)

	log.Println("RegionClient created with ping monitoring enabled")
	log.Println("Configuration:")
	log.Printf("  - Region Server: %s", *regionServer)
	log.Printf("  - Ping interval: %v", *interval)
	log.Printf("  - Latency window: %d measurements", *windowSize)
	log.Println("")
	log.Printf("Metrics available at: http://localhost:%s/metrics", *httpPort)
	log.Println("Look for metric: gohbase_ping_latency_seconds")
	log.Println("")

	// Dial to establish connection and start ping monitoring
	ctx := context.Background()
	err := client.Dial(ctx)
	if err != nil {
		log.Printf("Warning: Failed to dial region server: %v", err)
		log.Println("Continuing anyway for demonstration purposes...")
	} else {
		log.Println("Successfully connected to region server")
		log.Println("Ping monitoring started...")
	}

	// Keep the program running
	fmt.Println("\nPress Ctrl+C to exit...")
	
	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	
	log.Println("\nShutting down...")
	client.Close()
}