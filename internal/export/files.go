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

type FileLoader interface {
	GetFile(path string) ([]byte, error)
}

type Exporter struct {
	loader    FileLoader
	outputDir string
}

func NewExporter(loader FileLoader, outputDir string) *Exporter {
	return &Exporter{
		loader:    loader,
		outputDir: outputDir,
	}
}

type ProgressCallback func(current int, total int, description string)

type progressCounter struct {
	done  int
	total int
	mu    sync.Mutex
	cb    ProgressCallback
}

func (p *progressCounter) tick(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done++
	if p.cb != nil {
		p.cb(p.done, p.total, name)
	}
}

func (e *Exporter) ExportFiles(ctx context.Context, files []string, progressCallback ProgressCallback) (int, error) {
	if len(files) == 0 {
		return 0, nil
	}

	if err := os.MkdirAll(e.outputDir, 0755); err != nil {
		return 0, fmt.Errorf("creating output directory: %w", err)
	}

	files = lowerPaths(files)

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
	skipped := 0

	if len(spriteFiles) > 0 {
		ok, bad, skip, err := e.exportSprites(ctx, spriteFiles, progress)
		if err != nil {
			return exported, err
		}
		exported += ok
		failed += bad
		skipped += skip
	}

	ok, bad, skip, err := e.exportRegularFiles(ctx, regularFiles, progress)
	exported += ok
	failed += bad
	skipped += skip
	if err != nil {
		return exported, err
	}

	if skipped > 0 {
		slog.Info("Skipped already exported files", "count", skipped)
	}
	if failed > 0 {
		slog.Warn("Some files failed to export", "failed", failed, "exported", exported)
	}
	return exported, nil
}

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

func ResolveSpriteSheets(loader FileLoader, files []string) ([]string, error) {
	e := &Exporter{loader: loader}

	files = lowerPaths(files)

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

func (e *Exporter) exportSprites(ctx context.Context, spriteFiles []string, progress *progressCounter) (int, int, int, error) {
	index, err := e.spriteIndex()
	if err != nil {
		return 0, 0, 0, err
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

	var exported, failed, skipped atomic.Int64
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
					skipped.Add(1)
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
		return int(exported.Load()), int(failed.Load()), int(skipped.Load()), err
	}
	return int(exported.Load()), int(failed.Load()), int(skipped.Load()), nil
}

func (e *Exporter) exportRegularFiles(ctx context.Context, regularFiles []string, progress *progressCounter) (int, int, int, error) {
	if len(regularFiles) == 0 {
		return 0, 0, 0, nil
	}

	var exported, failed, skipped atomic.Int64

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.GOMAXPROCS(0))

	for _, filePath := range regularFiles {
		g.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			defer progress.tick(sanitizePath(filePath))

			t := transformFor(filePath)
			outputPath := filepath.Join(e.outputDir, sanitizePath(t.output(filePath)))

			if _, err := os.Stat(outputPath); err == nil {
				skipped.Add(1)
				return nil
			}

			fileData, err := e.loader.GetFile(filePath)
			if err != nil {
				slog.Warn("Skipping file export", "path", filePath, "error", err)
				failed.Add(1)
				return nil
			}

			if err := t.write(filePath, outputPath, fileData); err != nil {
				failed.Add(1)
				return nil
			}
			exported.Add(1)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return int(exported.Load()), int(failed.Load()), int(skipped.Load()), err
	}
	return int(exported.Load()), int(failed.Load()), int(skipped.Load()), nil
}

func sanitizePath(path string) string {
	return strings.ReplaceAll(path, "/", "@")
}

func lowerPaths(paths []string) []string {
	lowered := make([]string, len(paths))
	for i, p := range paths {
		lowered[i] = strings.ToLower(p)
	}
	return lowered
}
