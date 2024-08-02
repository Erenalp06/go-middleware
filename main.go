package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"sort"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
)

type Span struct {
	TraceID    string  `json:"trace_id"`
	SpanID     string  `json:"span_id"`
	ParentID   string  `json:"parent_id"`
	SpanName   string  `json:"span_name"`
	Timestamp  string  `json:"timestamp"`
	Duration   float64 `json:"duration_ms"`
	Children   []Span  `json:"children,omitempty"`
	Suspect    bool    `json:"suspect,omitempty"`
	SleepGuess float64 `json:"sleep_guess,omitempty"`
	TimeWindow string  `json:"time_window,omitempty"`
}

type EndpointStats struct {
	P99 float64
	P95 float64
	P90 float64
}

func main() {
	// ClickHouse bağlantısını oluşturun
	connect, err := sql.Open("clickhouse", "tcp://192.168.1.28:9000?username=default&password=1&database=otel")
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer connect.Close()

	// Zaman aralığını belirleyin
	startTime := "2024-07-30 20:00:00"
	endTime := "2024-07-30 21:00:00"
	windowSize := 10 * time.Minute

	startDate, _ := time.Parse("2006-01-02 15:04:05", startTime)
	endDate, _ := time.Parse("2006-01-02 15:04:05", endTime)

	windows, suspectSpans := detectAnomaliesInWindows(connect, startDate, endDate, windowSize)

	// Tüm span'leri içeren dosya oluşturun
	file, err := os.Create("traces.json")
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	// JSON verisini dosyaya yaz
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(windows); err != nil {
		log.Fatalf("Failed to write JSON to file: %v", err)
	}

	// Şüpheli span'leri içeren dosya oluşturun
	suspectFile, err := os.Create("suspect_traces.json")
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer suspectFile.Close()

	// Şüpheli span'lerin JSON verisini dosyaya yaz
	suspectEncoder := json.NewEncoder(suspectFile)
	suspectEncoder.SetIndent("", "  ")
	if err := suspectEncoder.Encode(suspectSpans); err != nil {
		log.Fatalf("Failed to write JSON to file: %v", err)
	}
}

// Pencereleri böl ve anomalileri tespit et
func detectAnomaliesInWindows(connect *sql.DB, startDate, endDate time.Time, windowSize time.Duration) ([][]Span, []Span) {
	var windows [][]Span
	var suspectSpans []Span

	for start := startDate; start.Before(endDate); start = start.Add(windowSize) {
		end := start.Add(windowSize)

		spans := fetchSpans(connect, start, end)
		endpointGroups := groupSpansByEndpoint(spans)

		var windowSpans []Span
		for _, spans := range endpointGroups {
			stats := calculatePercentiles(spans)
			detectAnomalies(spans, stats, &windowSpans, start, end, &suspectSpans)
		}
		windows = append(windows, windowSpans)
	}
	return windows, suspectSpans
}

// Verileri ClickHouse'dan çek
func fetchSpans(connect *sql.DB, start, end time.Time) []Span {
	rows, err := connect.Query(`
        SELECT TraceId, SpanId, ParentSpanId, SpanName, Duration / 1000000 AS Duration_ms, Timestamp
        FROM otel_traces
        WHERE SpanName LIKE 'POST%'
        AND Timestamp BETWEEN ? AND ?
        ORDER BY Timestamp
    `, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"))
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	var spans []Span
	for rows.Next() {
		var span Span
		if err := rows.Scan(&span.TraceID, &span.SpanID, &span.ParentID, &span.SpanName, &span.Duration, &span.Timestamp); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		spans = append(spans, span)
	}
	return spans
}

// Endpointlere göre gruplandır
func groupSpansByEndpoint(spans []Span) map[string][]Span {
	endpointGroups := make(map[string][]Span)
	for _, span := range spans {
		endpointGroups[span.SpanName] = append(endpointGroups[span.SpanName], span)
	}
	return endpointGroups
}

// Yüzdelik hesapla
func calculatePercentiles(spans []Span) EndpointStats {
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].Duration < spans[j].Duration
	})
	p99 := calculatePercentile(spans, 0.99)
	p95 := calculatePercentile(spans, 0.95)
	p90 := calculatePercentile(spans, 0.90)

	return EndpointStats{
		P99: p99,
		P95: p95,
		P90: p90,
	}
}

func calculatePercentile(spans []Span, percentile float64) float64 {
	index := int(percentile * float64(len(spans)))
	if index >= len(spans) {
		index = len(spans) - 1
	}
	return spans[index].Duration
}

func detectAnomalies(spans []Span, stats EndpointStats, windowSpans *[]Span, start, end time.Time, suspectSpans *[]Span) {
	timeWindow := start.Format("2006-01-02 15:04:05") + " - " + end.Format("2006-01-02 15:04:05")

	p99Threshold := stats.P99 * 3
	p95Threshold := stats.P95 * 3
	p90Threshold := stats.P90 * 3

	for i := range spans {
		spans[i].TimeWindow = timeWindow
		if spans[i].Duration > p99Threshold || spans[i].Duration > p95Threshold || spans[i].Duration > p90Threshold {
			spans[i].Suspect = true
			if spans[i].Duration > p99Threshold {
				spans[i].SleepGuess = spans[i].Duration - stats.P99
			} else if spans[i].Duration > p95Threshold {
				spans[i].SleepGuess = spans[i].Duration - stats.P95
			} else if spans[i].Duration > p90Threshold {
				spans[i].SleepGuess = spans[i].Duration - stats.P90
			}
			*suspectSpans = append(*suspectSpans, spans[i])
		}
		*windowSpans = append(*windowSpans, spans[i])
	}
}
