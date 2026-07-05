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

// DecodeDDS decodes DDS bytes into an image. Sprite export decodes each
// sheet once and crops many images from it.
func DecodeDDS(ddsData []byte) (image.Image, error) {
	img, err := dds.Decode(ddsData)
	if err != nil {
		return nil, fmt.Errorf("decoding DDS: %w", err)
	}
	return img, nil
}

// EncodePNG writes an image (optionally cropped) to outputPath as PNG.
func EncodePNG(img image.Image, crop *CropParams, outputPath string) error {
	if crop != nil {
		bounds := image.Rect(crop.Left, crop.Top, crop.Left+crop.Width, crop.Top+crop.Height)
		type subImager interface {
			SubImage(r image.Rectangle) image.Image
		}
		si, ok := img.(subImager)
		if !ok {
			return fmt.Errorf("image type %T does not support cropping", img)
		}
		sub := si.SubImage(bounds)
		if sub.Bounds().Empty() {
			return fmt.Errorf("crop %v is outside the image bounds %v", bounds, img.Bounds())
		}
		img = sub
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

// ConvertDDSToPNG decodes DDS bytes and writes them to outputPath as PNG.
func ConvertDDSToPNG(ddsData []byte, crop *CropParams, outputPath string) error {
	img, err := DecodeDDS(ddsData)
	if err != nil {
		return err
	}
	return EncodePNG(img, crop, outputPath)
}
