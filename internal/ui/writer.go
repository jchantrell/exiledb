package ui

import (
	"io"
	"sync"
)

// SwapWriter is an io.Writer whose destination can be swapped at runtime.
// It lets a long-lived logger redirect its output through a Progress
// container while bars are rendering, then back to stderr afterwards.
type SwapWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func NewSwapWriter(w io.Writer) *SwapWriter {
	return &SwapWriter{w: w}
}

func (s *SwapWriter) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(b)
}

func (s *SwapWriter) Swap(w io.Writer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.w = w
}
