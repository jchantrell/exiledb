package export

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

// ExportFiles exports the specified files from bundles to the output directory
// Handles sprite extraction and DDS conversion as needed
func (e *Exporter) ExportFiles(files []string, progressCallback ProgressCallback) error {
	if len(files) == 0 {
		return nil
	}

	// Create output directory
	if err := os.MkdirAll(e.outputDir, 0755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	totalFiles := len(files)
	processedCount := 0

	// Check if we have any sprite files
	hasSpriteFiles := false
	for _, file := range files {
		if IsInsideSprite(file) {
			hasSpriteFiles = true
			break
		}
	}

	// Load sprite indices only if needed
	var parsedLists [][]SpriteImage
	if hasSpriteFiles {
		var err error
		parsedLists, err = e.loadSpriteIndices()
		if err != nil {
			return fmt.Errorf("loading sprite indices: %w", err)
		}

		// Export from sprites
		processed, err := e.exportSprites(files, parsedLists, totalFiles, &processedCount, progressCallback)
		if err != nil {
			return fmt.Errorf("exporting sprites: %w", err)
		}
		processedCount = processed
	}

	// Export regular files
	_, err := e.exportRegularFiles(files, totalFiles, &processedCount, progressCallback)
	if err != nil {
		return fmt.Errorf("exporting regular files: %w", err)
	}

	return nil
}

// loadSpriteIndices loads and parses all sprite index files
func (e *Exporter) loadSpriteIndices() ([][]SpriteImage, error) {
	parsedLists := make([][]SpriteImage, len(SpriteLists))

	for i, sprite := range SpriteLists {
		slog.Debug("Loading sprite index", "path", sprite.Path)

		fileData, err := e.loader.GetFile(sprite.Path)
		if err != nil {
			return nil, fmt.Errorf("loading sprite index %s: %w", sprite.Path, err)
		}

		sprites, err := ParseSpriteIndex(fileData)
		if err != nil {
			return nil, fmt.Errorf("parsing sprite index %s: %w", sprite.Path, err)
		}

		parsedLists[i] = sprites
		slog.Debug("Loaded sprite index", "path", sprite.Path, "count", len(sprites))
	}

	return parsedLists, nil
}

// exportSprites exports images from sprite sheets
func (e *Exporter) exportSprites(files []string, parsedLists [][]SpriteImage, totalFiles int, processedCount *int, progressCallback ProgressCallback) (int, error) {
	// Filter files that are inside sprites
	spriteFiles := make([]string, 0)
	for _, file := range files {
		if IsInsideSprite(file) {
			spriteFiles = append(spriteFiles, file)
		}
	}

	if len(spriteFiles) == 0 {
		return *processedCount, nil
	}

	// Map files to their sprite images
	images := make([]*SpriteImage, 0, len(spriteFiles))
	for _, path := range spriteFiles {
		// Find which sprite list this file belongs to
		listIdx := -1
		for i, list := range SpriteLists {
			if strings.HasPrefix(path, list.NamePrefix) {
				listIdx = i
				break
			}
		}

		if listIdx == -1 {
			slog.Warn("File marked as sprite but no matching list found", "path", path)
			continue
		}

		// Find the sprite image in the parsed list
		var found *SpriteImage
		for i := range parsedLists[listIdx] {
			if parsedLists[listIdx][i].Name == path {
				found = &parsedLists[listIdx][i]
				break
			}
		}

		if found == nil {
			slog.Warn("Sprite image not found in index", "path", path)
			continue
		}

		images = append(images, found)
	}

	// Group images by sprite path
	bySprite := make(map[string][]*SpriteImage)
	for _, img := range images {
		bySprite[img.SpritePath] = append(bySprite[img.SpritePath], img)
	}

	// Process each sprite sheet
	for spritePath, spriteImages := range bySprite {
		slog.Info("Extracting sprite sheet", "path", spritePath, "image_count", len(spriteImages))

		// Load the DDS file
		ddsData, err := e.loader.GetFile(spritePath)
		if err != nil {
			return *processedCount, fmt.Errorf("loading sprite DDS %s: %w", spritePath, err)
		}

		// Extract each image from the sprite sheet
		for _, img := range spriteImages {
			outputPath := filepath.Join(e.outputDir, sanitizePath(img.Name)+".png")

			crop := &CropParams{
				Width:  img.Width,
				Height: img.Height,
				Top:    img.Top,
				Left:   img.Left,
			}

			if err := ConvertDDSToPNG(ddsData, crop, outputPath); err != nil {
				return *processedCount, fmt.Errorf("converting sprite image %s: %w", img.Name, err)
			}

			*processedCount++
			if progressCallback != nil {
				progressCallback(*processedCount, totalFiles, sanitizePath(img.Name))
			}

			slog.Debug("Extracted sprite image", "name", img.Name, "output", outputPath)
		}
	}

	return *processedCount, nil
}

// exportRegularFiles exports non-sprite files
func (e *Exporter) exportRegularFiles(files []string, totalFiles int, processedCount *int, progressCallback ProgressCallback) (int, error) {
	// Filter out sprite files
	regularFiles := make([]string, 0)
	for _, file := range files {
		if !IsInsideSprite(file) {
			regularFiles = append(regularFiles, file)
		}
	}

	if len(regularFiles) == 0 {
		return *processedCount, nil
	}

	for _, filePath := range regularFiles {
		fileData, err := e.loader.GetFile(filePath)
		if err != nil {
			return *processedCount, fmt.Errorf("loading file %s: %w", filePath, err)
		}

		// Determine output path and processing
		var outputPath string
		if strings.HasSuffix(filePath, ".dds") {
			// Convert DDS to PNG
			outputPath = filepath.Join(e.outputDir, strings.TrimSuffix(sanitizePath(filePath), ".dds")+".png")

			if err := ConvertDDSToPNG(fileData, nil, outputPath); err != nil {
				return *processedCount, fmt.Errorf("converting DDS file %s: %w", filePath, err)
			}

			slog.Debug("Converted DDS to PNG", "path", filePath, "output", outputPath)
		} else {
			// Copy file (decode text files to UTF-8)
			outputPath = filepath.Join(e.outputDir, sanitizePath(filePath))

			// Decode UTF-16LE text files to UTF-8 for human readability
			if strings.HasSuffix(strings.ToLower(filePath), ".txt") || strings.HasSuffix(strings.ToLower(filePath), ".text") {
				text, err := DecodeUTF16LE(fileData)
				if err != nil {
					return *processedCount, fmt.Errorf("decoding UTF-16LE file %s: %w", filePath, err)
				}
				fileData = []byte(text)
				slog.Debug("Decoded text file to UTF-8", "path", filePath, "output", outputPath)
			}

			if err := os.WriteFile(outputPath, fileData, 0644); err != nil {
				return *processedCount, fmt.Errorf("writing file %s: %w", outputPath, err)
			}

			slog.Debug("Copied file", "path", filePath, "output", outputPath)
		}

		// Update progress for all files
		*processedCount++
		if progressCallback != nil {
			progressCallback(*processedCount, totalFiles, sanitizePath(filePath))
		}
	}

	return *processedCount, nil
}

// sanitizePath sanitizes a file path for use as a filename
// Replaces forward slashes with @ symbols
func sanitizePath(path string) string {
	return strings.ReplaceAll(path, "/", "@")
}
