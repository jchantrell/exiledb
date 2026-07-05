// Package ui owns all terminal presentation: progress bars and routing of
// log output so bars and logs never write over each other.
package ui

import (
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"golang.org/x/term"
)

const (
	labelWidth = 30
	// Space consumed by decorators: percentage, counters, separators, label.
	decorWidth   = labelWidth + 28
	maxBarWidth  = 64
	minBarWidth  = 10
	refreshEvery = 100 * time.Millisecond
)

// Progress hosts every progress bar for a single command run in one mpb
// container. A disabled Progress (no TTY, --no-progress, JSON logs) is fully
// functional: every method is a no-op and LogWriter falls back to stderr.
type Progress struct {
	container *mpb.Progress
	mu        sync.Mutex
	bars      []*mpb.Bar
}

func NewProgress(enabled bool) *Progress {
	if !enabled || !term.IsTerminal(int(os.Stderr.Fd())) {
		return &Progress{}
	}
	return &Progress{
		container: mpb.New(
			mpb.WithOutput(os.Stderr),
			mpb.WithWidth(barWidth()),
			mpb.WithRefreshRate(refreshEvery),
		),
	}
}

// LogWriter returns the writer logs must go through while bars are live.
// Lines written to it render above any running bar instead of through it.
func (p *Progress) LogWriter() io.Writer {
	if p.container == nil {
		return os.Stderr
	}
	return p.container
}

// Phase returns a reporter for one phase of work, safe for concurrent use.
// The bar is created on the first report (a phase with zero items never
// shows a bar) and removes itself once done reaches total. The unnamed
// return type keeps it assignable to each package's own callback type.
func (p *Progress) Phase() func(done, total int, label string) {
	if p.container == nil {
		return func(int, int, string) {}
	}

	var (
		once  sync.Once
		bar   *mpb.Bar
		label atomic.Value
	)
	label.Store("")

	return func(done, total int, l string) {
		once.Do(func() {
			bar = p.newBar(int64(total), &label)
			p.mu.Lock()
			p.bars = append(p.bars, bar)
			p.mu.Unlock()
		})
		label.Store(l)
		bar.SetTotal(int64(total), false)
		bar.SetCurrent(int64(done))
	}
}

// Close drops any unfinished bar and waits for the final render. Swap log
// output back to stderr before calling Close: writes to the container after
// shutdown are discarded.
func (p *Progress) Close() {
	if p.container == nil {
		return
	}
	p.mu.Lock()
	for _, bar := range p.bars {
		bar.Abort(true)
	}
	p.mu.Unlock()
	p.container.Wait()
}

func (p *Progress) newBar(total int64, label *atomic.Value) *mpb.Bar {
	return p.container.New(total,
		mpb.BarStyle().Lbound("[").Filler("█").Tip("█").Padding("░").Rbound("]"),
		mpb.BarRemoveOnComplete(),
		mpb.AppendDecorators(
			decor.Percentage(),
			decor.Name(" | "),
			decor.CountersNoUnit("%d/%d"),
			decor.Name(" | "),
			decor.Any(func(decor.Statistics) string {
				return truncate(label.Load().(string), labelWidth)
			}, decor.WC{W: labelWidth, C: decor.DindentRight}),
		),
	)
}

// barWidth clamps the bar filler so bar plus decorators never exceed the
// terminal width, which would wrap the line and corrupt repaints.
func barWidth() int {
	w, _, err := term.GetSize(int(os.Stderr.Fd()))
	if err != nil || w <= 0 {
		return maxBarWidth
	}
	return max(minBarWidth, min(maxBarWidth, w-decorWidth))
}

func truncate(s string, limit int) string {
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit-2]) + ".."
}
