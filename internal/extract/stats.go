package extract

import (
	"fmt"
	"io"
	"runtime"
	"strings"
	"time"
)

// Stats accumulates the outcome of one extraction run.
type Stats struct {
	StartTime        time.Time
	EndTime          time.Time
	TotalTables      int
	ProcessedTables  int
	RowsInserted     int64
	ProcessingErrors int
	DatabaseErrors   int
	FilesExported    int

	processingStart time.Time
}

// Report writes the human-readable run summary.
func (s *Stats) Report(w io.Writer) {
	totalDuration := s.EndTime.Sub(s.StartTime)
	processingDuration := s.EndTime.Sub(s.processingStart)

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	totalMemoryMB := float64(memStats.Alloc) / 1024.0 / 1024.0

	var tableProcessingRate, rowInsertionRate float64
	if seconds := processingDuration.Seconds(); seconds > 0 {
		tableProcessingRate = float64(s.ProcessedTables) / seconds
		rowInsertionRate = float64(s.RowsInserted) / seconds
	}

	if s.TotalTables > 0 {
		successRate := float64(s.ProcessedTables) / float64(s.TotalTables) * 100
		fmt.Fprintf(w, "Tables processed: %d/%d (%.1f%%)\n", s.ProcessedTables, s.TotalTables, successRate)
	}
	fmt.Fprintf(w, "Rows inserted: %s\n", formatNumber(s.RowsInserted))
	fmt.Fprintf(w, "Files exported: %d\n", s.FilesExported)
	fmt.Fprintf(w, "Processing errors: %d\n", s.ProcessingErrors)
	fmt.Fprintf(w, "Database errors: %d\n", s.DatabaseErrors)
	fmt.Fprintf(w, "Total duration: %.1fms\n", float64(totalDuration.Nanoseconds())/1000000.0)
	fmt.Fprintf(w, "Processing duration: %.1fms\n", float64(processingDuration.Nanoseconds())/1000000.0)
	fmt.Fprintf(w, "Processing rate: %.2f tables/sec\n", tableProcessingRate)
	fmt.Fprintf(w, "Insertion rate: %s rows/sec\n", formatRate(rowInsertionRate))
	fmt.Fprintf(w, "Memory usage: %.2fmb\n", totalMemoryMB)
}

// formatNumber formats large numbers with commas for readability.
func formatNumber(n int64) string {
	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return str
	}

	var result []string
	for i, digit := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result = append(result, ",")
		}
		result = append(result, string(digit))
	}
	return strings.Join(result, "")
}

// formatRate formats insertion rates with unit suffixes.
func formatRate(rate float64) string {
	if rate < 1000 {
		return fmt.Sprintf("%.2f", rate)
	} else if rate < 1000000 {
		return fmt.Sprintf("%.2fK", rate/1000)
	}
	return fmt.Sprintf("%.2fM", rate/1000000)
}
