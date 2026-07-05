package export

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// FileLoader defines the interface for loading files from bundles
type FileLoader interface {
	GetFile(path string) ([]byte, error)
}

// Exporter handles exporting files from bundles to disk
type Exporter struct {
	loader    FileLoader
	outputDir string
}

// NewExporter creates a new file exporter
func NewExporter(loader FileLoader, outputDir string) *Exporter {
	return &Exporter{
		loader:    loader,
		outputDir: outputDir,
	}
}

// ProgressCallback is called to report export progress
type ProgressCallback func(current int, total int, description string)

// progressCounter is the single shared progress state for an export run.
type progressCounter struct {
	done  atomic.Int64
	total int
	mu    sync.Mutex
	cb    ProgressCallback
}

func (p *progressCounter) tick(name string) {
	cur := int(p.done.Add(1))
	if p.cb != nil {
		p.mu.Lock()
		p.cb(cur, p.total, name)
		p.mu.Unlock()
	}
}

// ExportFiles exports the specified files from bundles to the output
// directory, handling sprite extraction and DDS conversion as needed.
// Individual file failures are logged and counted rather than aborting the
// run; the returned count is the number of files actually exported (or
// already present).
func (e *Exporter) ExportFiles(ctx context.Context, files []string, progressCallback ProgressCallback) (int, error) {
	if len(files) == 0 {
		return 0, nil
	}

	if err := os.MkdirAll(e.outputDir, 0755); err != nil {
		return 0, fmt.Errorf("creating output directory: %w", err)
	}

	var spriteFiles, regularFiles []string
	for _, file := range files {
		if IsInsideSprite(file) {
			spriteFiles = append(spriteFiles, file)
		} else {
			regularFiles = append(regularFiles, file)
		}
	}

	progress := &progressCounter{total: len(files), cb: progressCallback}
	exported := 0
	failed := 0

	if len(spriteFiles) > 0 {
		ok, bad, err := e.exportSprites(ctx, spriteFiles, progress)
		if err != nil {
			return exported, err
		}
		exported += ok
		failed += bad
	}

	ok, bad, err := e.exportRegularFiles(ctx, regularFiles, progress)
	exported += ok
	failed += bad
	if err != nil {
		return exported, err
	}

	if failed > 0 {
		slog.Warn("Some files failed to export", "failed", failed, "exported", exported)
	}
	return exported, nil
}

// spriteIndex maps image names to their entries across all sprite lists.
func (e *Exporter) spriteIndex() (map[string]*SpriteImage, error) {
	index := make(map[string]*SpriteImage)
	for _, list := range SpriteLists {
		fileData, err := e.loader.GetFile(list.Path)
		if err != nil {
			slog.Warn("Sprite index not available, skipping", "path", list.Path, "error", err)
			continue
		}

		sprites, err := ParseSpriteIndex(fileData)
		if err != nil {
			return nil, fmt.Errorf("parsing sprite index %s: %w", list.Path, err)
		}
		slog.Debug("Loaded sprite index", "path", list.Path, "count", len(sprites))

		for i := range sprites {
			index[sprites[i].Name] = &sprites[i]
		}
	}
	return index, nil
}

// ResolveSpriteSheets returns the sheet DDS paths needed to export the given
// sprite-image names. Callers use it to discover which bundles must be
// downloaded before export can run.
func ResolveSpriteSheets(loader FileLoader, files []string) ([]string, error) {
	e := &Exporter{loader: loader}

	needed := false
	for _, f := range files {
		if IsInsideSprite(f) {
			needed = true
			break
		}
	}
	if !needed {
		return nil, nil
	}

	index, err := e.spriteIndex()
	if err != nil {
		return nil, err
	}

	sheets := make(map[string]bool)
	for _, f := range files {
		if img, ok := index[f]; ok {
			sheets[img.SpritePath] = true
		}
	}

	paths := make([]string, 0, len(sheets))
	for sheet := range sheets {
		paths = append(paths, sheet)
	}
	sort.Strings(paths)
	return paths, nil
}

// exportSprites exports images cropped from sprite sheets. Each sheet is
// decoded once and processed by one worker; a failing sheet fails only its
// own images. Returns (exported, failed).
func (e *Exporter) exportSprites(ctx context.Context, spriteFiles []string, progress *progressCounter) (int, int, error) {
	index, err := e.spriteIndex()
	if err != nil {
		return 0, 0, err
	}

	bySheet := make(map[string][]*SpriteImage)
	var missing int
	for _, path := range spriteFiles {
		img, ok := index[path]
		if !ok {
			slog.Warn("Sprite image not found in index", "path", path)
			missing++
			progress.tick(sanitizePath(path))
			continue
		}
		bySheet[img.SpritePath] = append(bySheet[img.SpritePath], img)
	}

	sheets := make([]string, 0, len(bySheet))
	for sheet := range bySheet {
		sheets = append(sheets, sheet)
	}
	sort.Strings(sheets)

	var exported, failed atomic.Int64
	failed.Add(int64(missing))

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.GOMAXPROCS(0))

	for _, sheet := range sheets {
		images := bySheet[sheet]
		g.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			slog.Info("Extracting sprite sheet", "path", sheet, "image_count", len(images))

			ddsData, err := e.loader.GetFile(sheet)
			if err != nil {
				slog.Error("Failed to load sprite sheet", "path", sheet, "error", err)
				failed.Add(int64(len(images)))
				for _, img := range images {
					progress.tick(sanitizePath(img.Name))
				}
				return nil
			}

			decoded, err := DecodeDDS(ddsData)
			if err != nil {
				slog.Error("Failed to decode sprite sheet", "path", sheet, "error", err)
				failed.Add(int64(len(images)))
				for _, img := range images {
					progress.tick(sanitizePath(img.Name))
				}
				return nil
			}

			for _, img := range images {
				outputPath := filepath.Join(e.outputDir, sanitizePath(img.Name)+".png")
				if _, err := os.Stat(outputPath); err == nil {
					exported.Add(1)
					progress.tick(sanitizePath(img.Name))
					continue
				}

				crop := &CropParams{Left: img.Left, Top: img.Top, Width: img.Width, Height: img.Height}
				if err := EncodePNG(decoded, crop, outputPath); err != nil {
					slog.Error("Failed to export sprite image", "name", img.Name, "error", err)
					failed.Add(1)
				} else {
					exported.Add(1)
					slog.Debug("Extracted sprite image", "name", img.Name, "output", outputPath)
				}
				progress.tick(sanitizePath(img.Name))
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return int(exported.Load()), int(failed.Load()), err
	}
	return int(exported.Load()), int(failed.Load()), nil
}

// exportRegularFiles exports non-sprite files using parallel workers.
// Returns (exported, failed).
func (e *Exporter) exportRegularFiles(ctx context.Context, regularFiles []string, progress *progressCounter) (int, int, error) {
	if len(regularFiles) == 0 {
		return 0, 0, nil
	}

	var exported, failed, skipped atomic.Int64

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.GOMAXPROCS(0))

	for _, filePath := range regularFiles {
		var outputPath string
		if strings.HasSuffix(filePath, ".dds") {
			outputPath = filepath.Join(e.outputDir, strings.TrimSuffix(sanitizePath(filePath), ".dds")+".png")
		} else {
			outputPath = filepath.Join(e.outputDir, sanitizePath(filePath))
		}

		if _, err := os.Stat(outputPath); err == nil {
			skipped.Add(1)
			exported.Add(1)
			progress.tick(sanitizePath(filePath))
			continue
		}

		g.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			defer progress.tick(sanitizePath(filePath))

			fileData, err := e.loader.GetFile(filePath)
			if err != nil {
				slog.Warn("Skipping file export", "path", filePath, "error", err)
				failed.Add(1)
				return nil
			}

			if strings.HasSuffix(filePath, ".dds") {
				if err := ConvertDDSToPNG(fileData, nil, outputPath); err != nil {
					slog.Warn("Skipping DDS conversion", "path", filePath, "error", err)
					failed.Add(1)
					return nil
				}
				slog.Debug("Converted DDS to PNG", "path", filePath, "output", outputPath)
				exported.Add(1)
				return nil
			}

			data := fileData
			lower := strings.ToLower(filePath)
			if strings.HasSuffix(lower, ".txt") || strings.HasSuffix(lower, ".text") {
				text, err := DecodeUTF16LE(data)
				if err != nil {
					slog.Debug("Text file is not UTF-16LE, writing as-is", "path", filePath, "error", err)
				} else {
					data = []byte(text)
					slog.Debug("Decoded text file to UTF-8", "path", filePath, "output", outputPath)
				}
			} else if strings.HasSuffix(lower, ".ast") {
				decompressed, err := DecompressAST(data)
				if err != nil {
					slog.Warn("AST decompression failed, writing as-is", "path", filePath, "error", err)
				} else {
					data = decompressed
					slog.Debug("Decompressed AST animation payload", "path", filePath)
				}
			}

			if err := os.WriteFile(outputPath, data, 0644); err != nil {
				slog.Error("Failed to write file", "path", outputPath, "error", err)
				failed.Add(1)
				return nil
			}
			slog.Debug("Copied file", "path", filePath, "output", outputPath)
			exported.Add(1)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return int(exported.Load()), int(failed.Load()), err
	}

	if s := skipped.Load(); s > 0 {
		slog.Info("Skipped already exported files", "count", s)
	}
	return int(exported.Load()), int(failed.Load()), nil
}

// sanitizePath sanitizes a file path for use as a filename
// Replaces forward slashes with @ symbols
func sanitizePath(path string) string {
	return strings.ReplaceAll(path, "/", "@")
}
