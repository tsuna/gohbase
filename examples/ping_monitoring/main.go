// Example of using the scan control functionality with RegionClient
// This example demonstrates how to enable scan control with congestion monitoring
// for a RegionClient and expose the metrics via HTTP endpoint for Prometheus scraping.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
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
		interval     = flag.Duration("interval", 10*time.Second, "Ping interval for congestion control")
		maxScans     = flag.Int("max-scans", 100, "Maximum concurrent scans")
		minScans     = flag.Int("min-scans", 10, "Minimum concurrent scans")
		maxLatency   = flag.Duration("max-latency", 500*time.Millisecond, "Maximum acceptable latency")
		minLatency   = flag.Duration("min-latency", 100*time.Millisecond, "Minimum acceptable latency")
		httpPort     = flag.String("port", "2112", "HTTP port for Prometheus metrics")
		debug        = flag.Bool("debug", false, "Enable debug logging")
	)
	
	flag.Parse()
	
	// Set up logging
	var logger *slog.Logger
	if *debug {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
		log.Println("Debug logging enabled")
	} else {
		logger = slog.Default()
	}

	// Start Prometheus metrics HTTP server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		addr := ":" + *httpPort
		log.Printf("Prometheus metrics available at http://localhost:%s/metrics", *httpPort)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatal("Failed to start metrics server:", err)
		}
	}()

	// Create a RegionClient with scan control enabled
	options := &region.RegionClientOptions{
		Logger: logger,
		ScanControl: &region.ScanControlOptions{
			MaxScans: *maxScans,
			MinScans: *minScans,
			MaxLat:   *maxLatency,
			MinLat:   *minLatency,
			Interval: *interval,
		},
	}
	client := region.NewClient(*regionServer, region.RegionClient, options)

	log.Println("RegionClient created with scan control and congestion monitoring enabled")
	log.Println("Configuration:")
	log.Printf("  - Region Server: %s", *regionServer)
	log.Printf("  - Ping interval: %v", *interval)
	log.Printf("  - Max concurrent scans: %d", *maxScans)
	log.Printf("  - Min concurrent scans: %d", *minScans)
	log.Printf("  - Max latency threshold: %v", *maxLatency)
	log.Printf("  - Min latency threshold: %v", *minLatency)
	log.Println("")
	log.Printf("Metrics available at: http://localhost:%s/metrics", *httpPort)
	log.Println("Look for metrics: gohbase_ping_latency_seconds, gohbase_concurrent_scans")
	log.Println("")

	// Dial to establish connection and start ping monitoring
	ctx := context.Background()
	err := client.Dial(ctx)
	if err != nil {
		log.Printf("Warning: Failed to dial region server: %v", err)
		log.Println("Continuing anyway for demonstration purposes...")
	} else {
		log.Println("Successfully connected to region server")
		log.Println("Scan control with congestion monitoring started...")
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