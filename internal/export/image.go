package export

import (
	"bytes"
	"fmt"
	"os/exec"
)

// CropParams defines optional crop parameters for image extraction
type CropParams struct {
	Width  int
	Height int
	Top    int
	Left   int
}

// ConvertDDSToPNG converts a DDS image to PNG format using ImageMagick
// Optionally crops the image if crop parameters are provided
// Returns an error if ImageMagick is not installed or conversion fails
func ConvertDDSToPNG(ddsData []byte, crop *CropParams, outputPath string) error {
	// Build crop argument
	cropArg := "100%"
	if crop != nil {
		cropArg = fmt.Sprintf("%dx%d+%d+%d", crop.Width, crop.Height, crop.Top, crop.Left)
	}

	// Create ImageMagick command
	// Using 'magick' command (ImageMagick 7+)
	cmd := exec.Command("magick", "dds:-", "-crop", cropArg, outputPath)

	// Set up stdin to pipe DDS data
	cmd.Stdin = bytes.NewReader(ddsData)

	// Run the command
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("imagemagick exited with code %d", exitErr.ExitCode())
		}
		// Check if command not found
		if err == exec.ErrNotFound || err.Error() == "executable file not found in $PATH" {
			return fmt.Errorf("ImageMagick is not installed or not found in PATH: %w", err)
		}
		return fmt.Errorf("running imagemagick: %w", err)
	}

	return nil
}
