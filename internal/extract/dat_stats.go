package extract

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path"
	"strings"

	"github.com/jchantrell/exiledb/internal/bundle"
	"github.com/jchantrell/exiledb/internal/cdn"
	"github.com/jchantrell/exiledb/internal/config"
	"github.com/jchantrell/exiledb/internal/dat"
	"github.com/jchantrell/exiledb/internal/poe"
)

func isDatFile(p string) bool {
	ext := strings.ToLower(path.Ext(p))
	for _, e := range poe.DatExtensions {
		if ext == e {
			return true
		}
	}
	return false
}

var languageDirs = func() map[string]string {
	m := make(map[string]string)
	for _, l := range config.SupportedLanguages() {
		if l == config.LanguageEnglish {
			continue
		}
		m[strings.ToLower(l)] = l
	}
	return m
}()

func datFileLanguage(p string) string {
	for _, seg := range strings.Split(p, "/") {
		if lang, ok := languageDirs[seg]; ok {
			return lang
		}
	}
	return config.LanguageEnglish
}

type datStat struct {
	Path      string `json:"path"`
	RowCount  int    `json:"row_count"`
	RowWidth  int    `json:"row_width"`
	FixedSize int    `json:"fixed_size"`
	VarOffset int    `json:"var_offset"`
	VarSize   int    `json:"var_size"`
	SHA256    string `json:"sha256"`
}

// WriteDatStats streams one JSON object per dat table file (JSONL), sorted by
// path, to w. Each line carries the file's schema-free structural metrics plus
// a sha256 of its decompressed bytes: structural fields reveal what changed
// (rows, columns, variable data), while the hash catches in-place value edits
// that leave every size identical.
func WriteDatStats(ctx context.Context, cfg *config.Config, w io.Writer) error {
	gameVersion := 0
	if cfg.GgpkPath == "" {
		var err error
		gameVersion, err = poe.ParseGameVersion(cfg.Patch)
		if err != nil {
			return fmt.Errorf("parsing game version: %w", err)
		}
	}

	src, err := resolveSource(ctx, cfg, gameVersion, false)
	if err != nil {
		return err
	}
	defer src.bundleSource.Close()

	manager, err := bundle.NewBundleManager(src.bundleSource)
	if err != nil {
		return fmt.Errorf("creating bundle manager: %w", err)
	}
	defer manager.Close()

	wantLanguage := make(map[string]bool, len(cfg.Languages))
	for _, l := range cfg.Languages {
		wantLanguage[l] = true
	}

	index := manager.Index()
	var paths []string
	for _, p := range index.ListFilesWithPrefix("data") {
		if isDatFile(p) && wantLanguage[datFileLanguage(p)] {
			paths = append(paths, p)
		}
	}

	if src.cache != nil {
		bundles := bundlesForFiles(index, paths)
		if err := cdn.DownloadBundles(ctx, src.cache, cfg.Patch, gameVersion, bundles, false, func(int, int, string) {}); err != nil {
			return fmt.Errorf("downloading bundles: %w", err)
		}
	}

	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	for _, p := range paths {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		data, err := manager.GetFile(p)
		if err != nil {
			slog.Warn("Skipping dat file: read failed", "path", p, "error", err)
			continue
		}

		st, err := dat.ParseStructure(data)
		if err != nil {
			slog.Warn("Skipping dat file: structure parse failed", "path", p, "error", err)
			continue
		}

		sum := sha256.Sum256(data)
		if err := enc.Encode(datStat{
			Path:      p,
			RowCount:  st.RowCount,
			RowWidth:  st.RowWidth,
			FixedSize: st.FixedSize,
			VarOffset: st.VarOffset,
			VarSize:   st.VarSize,
			SHA256:    hex.EncodeToString(sum[:]),
		}); err != nil {
			return fmt.Errorf("writing dat stats: %w", err)
		}
	}

	return bw.Flush()
}
