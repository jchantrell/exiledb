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

// datFileExtensions are the boundary-marker table formats. The extension label
// has changed across patches (.dat64 -> .datc64, with .datcl64 for language
// variants) while the on-disk format has not.
var datFileExtensions = map[string]bool{
	".dat64":   true,
	".datc64":  true,
	".datcl64": true,
}

func isDatFile(p string) bool {
	return datFileExtensions[strings.ToLower(path.Ext(p))]
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

	index := manager.Index()
	var paths []string
	for _, p := range index.ListFilesWithPrefix("data") {
		if isDatFile(p) {
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
