package export

import (
	"fmt"
	"image"
	"image/png"
	"os"

	"github.com/jchantrell/exiledb/internal/dds"
)

type CropParams struct {
	Width  int
	Height int
	Top    int
	Left   int
}

func ConvertDDSToPNG(ddsData []byte, crop *CropParams, outputPath string) error {
	img, err := dds.Decode(ddsData)
	if err != nil {
		return fmt.Errorf("decoding DDS: %w", err)
	}

	if crop != nil {
		bounds := image.Rect(crop.Left, crop.Top, crop.Left+crop.Width, crop.Top+crop.Height)
		type subImager interface {
			SubImage(r image.Rectangle) image.Image
		}
		si, ok := img.(subImager)
		if !ok {
			return fmt.Errorf("image type %T does not support cropping", img)
		}
		img = si.SubImage(bounds)
	}

	f, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}
	defer f.Close()

	enc := &png.Encoder{CompressionLevel: png.BestSpeed}
	if err := enc.Encode(f, img); err != nil {
		return fmt.Errorf("encoding PNG: %w", err)
	}

	return nil
}
