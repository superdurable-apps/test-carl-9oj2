package process

import (
	"sort"
	"strings"
	"time"
)

// HistoricalItem is one timestamped, public signal supplied to a blind replay.
type HistoricalItem struct {
	Time   time.Time
	Title  string
	Text   string
	Author string
	Score  int
}

type BacktestOutcome struct {
	Detected       bool
	DetectedAt     time.Time
	LeadHours      float64
	Interpretation string
}

// Backtest detects every term before comparing the result with the named hotspot.
func Backtest(items []HistoricalItem, knownHotspot string, widespreadAt time.Time) BacktestOutcome {
	ordered := append([]HistoricalItem(nil), items...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Time.Before(ordered[j].Time) })
	target := strings.ToLower(strings.TrimSpace(knownHotspot))
	for index, item := range ordered {
		window := make([]RadarEvidence, 0, index+1)
		for _, observed := range ordered[:index+1] {
			if observed.Time.Before(item.Time.Add(-48 * time.Hour)) {
				continue
			}
			window = append(window, RadarEvidence{Title: observed.Title, Text: observed.Text, Author: observed.Author, Score: observed.Score, ObservedAt: observed.Time})
		}
		candidate, found := candidateCounts(window)[target]
		if !found || candidate.Frequency < 2 || candidate.Authors < 2 || candidate.Score < 20 {
			continue
		}
		lead := widespreadAt.Sub(item.Time).Hours()
		interpretation := "at broad discussion"
		if lead > 0 {
			interpretation = "early"
		} else if lead < 0 {
			interpretation = "late"
		}
		return BacktestOutcome{Detected: true, DetectedAt: item.Time, LeadHours: lead, Interpretation: interpretation}
	}
	return BacktestOutcome{Interpretation: "undetected"}
}
