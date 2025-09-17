package utils

import (
	"fmt"
	"os"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"golang.org/x/term"
)

// Progress represents a progress bar using mpb
type Progress struct {
	container   *mpb.Progress
	bar         *mpb.Bar
	enabled     bool
	description string
}

var descLength = 20

// NewProgress creates a new progress bar with the given total count
func NewProgress(total int, enabled bool) *Progress {
	isTerm := isTerminal()

	var container *mpb.Progress
	var bar *mpb.Bar

	p := &Progress{
		container:   container,
		bar:         bar,
		enabled:     enabled && isTerm,
		description: "",
	}

	if enabled && isTerm {
		// Add space before progress bar
		fmt.Fprintln(os.Stderr)

		// Create mpb container that outputs to stderr
		container = mpb.New(
			mpb.WithOutput(os.Stderr),
			mpb.WithWidth(64),
			mpb.WithRefreshRate(100*time.Millisecond),
		)

		// Create progress bar with decorators including dynamic description
		bar = container.New(int64(total),
			mpb.BarStyle().Lbound("[").Filler("█").Tip("█").Padding("░").Rbound("]"),
			mpb.PrependDecorators(
				decor.Any(func(statistics decor.Statistics) string {
					if len(p.description) > descLength {
						return p.description[:descLength-2] + ".."
					}
					return p.description
				}, decor.WC{W: descLength, C: decor.DindentRight}),
				decor.Name("  "),
				decor.CountersNoUnit("%d/%d", decor.WC{C: decor.DindentRight}),
			),
			mpb.AppendDecorators(
				decor.Percentage(),
			),
		)

		p.container = container
		p.bar = bar
	}

	return p
}

// SetEnabled allows manually enabling/disabling the progress bar
func (p *Progress) SetEnabled(enabled bool) {
	p.enabled = enabled
	if !enabled && p.container != nil {
		p.container.Shutdown()
		p.container = nil
		p.bar = nil
	}
}

// Update updates the progress bar with current count and description
func (p *Progress) Update(current int, description string) {
	if !p.enabled || p.bar == nil {
		return
	}

	// Update the description which will be shown by the dynamic decorator
	p.description = description

	// Set the bar to the current value
	p.bar.SetCurrent(int64(current))
}

// Finish completes the progress bar and shuts down the container
func (p *Progress) Finish() {
	if !p.enabled || p.container == nil {
		return
	}

	// Wait for the progress bar to finish and shutdown
	p.container.Wait()

	// Add space after progress bar
	fmt.Fprintln(os.Stderr)
}

// isTerminal checks if stderr is a terminal (TTY)
func isTerminal() bool {
	return term.IsTerminal(int(os.Stderr.Fd()))
}
