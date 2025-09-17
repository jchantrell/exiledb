package utils

import (
	"fmt"
	"strings"
	"time"
)

// Number formats large numbers with commas for readability.
// For example: 1234567 becomes "1,234,567"
func Number(n int64) string {
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

// Duration formats time duration in human-readable form.
// Examples:
//   - Less than 1 second: "0s"
//   - Less than 1 minute: "5.2s"
//   - Less than 1 hour: "3m5.2s"
//   - 1 hour or more: "2h15m"
func Duration(d time.Duration) string {
	if d < time.Second {
		return "0s"
	} else if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	} else if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := d.Seconds() - float64(minutes*60)
		return fmt.Sprintf("%dm%.1fs", minutes, seconds)
	} else {
		hours := int(d.Hours())
		minutes := int(d.Minutes()) % 60
		return fmt.Sprintf("%dh%dm", hours, minutes)
	}
}

// Rate formats insertion rates for readability with appropriate unit suffixes.
// Examples:
//   - Less than 1,000: "123.45"
//   - Less than 1,000,000: "12.34K"
//   - 1,000,000 or more: "12.34M"
func Rate(rate float64) string {
	if rate < 1000 {
		return fmt.Sprintf("%.2f", rate)
	} else if rate < 1000000 {
		return fmt.Sprintf("%.2fK", rate/1000)
	} else {
		return fmt.Sprintf("%.2fM", rate/1000000)
	}
}

