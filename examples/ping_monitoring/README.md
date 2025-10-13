# Ping Monitoring Example

This example demonstrates how to use the ping monitoring functionality in gohbase RegionClient.

## Features

- Sends periodic dummy Scan requests to measure latency
- Maintains a rolling window of latest measurements
- Exports metrics via Prometheus histogram

## Usage

```bash
go run main.go [options]

Options:
  -regionserver string
        HBase region server address (default "localhost:16020")
  -interval duration
        Ping interval (default 10s)
  -window int
        Number of latest ping measurements to keep (default 10)
  -port string
        HTTP port for Prometheus metrics (default "2112")
```

## Examples

Run with default settings:
```bash
go run main.go
```

Run with custom settings:
```bash
go run main.go -regionserver=hbase.example.com:16020 -interval=5s -window=20 -port=8080
```

## Metrics

The ping latency is exported as a Prometheus histogram:
- Metric name: `gohbase_ping_latency_seconds`
- Labels: `regionserver` (the address of the region server)

Access metrics at: `http://localhost:<port>/metrics`

## Notes

- The ping functionality uses dummy Scan requests with a minimal range
- Latencies are stored in a ring buffer for efficiency
- Set interval to 0 to disable ping monitoring