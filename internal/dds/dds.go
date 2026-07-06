package dds

import (
	"bytes"
	"errors"
	"fmt"
	"image"
	"image/color"
	"io"
)

type imageInfo struct {
	header      header
	dxt10Header *dxt10Header
	decompress  decompressFunc
	colorModel  color.Model
}

type blockCodec struct {
	colorModel color.Model
	decompress decompressFunc
}

var (
	codecDXT1    = blockCodec{color.NRGBAModel, decompressDXT1}
	codecDXT5    = blockCodec{color.NRGBAModel, decompressDXT5}
	codec3DcPlus = blockCodec{color.GrayModel, decompress3DcPlus}
	codec3Dc     = blockCodec{color.NRGBAModel, decompress3Dc}
	codecBC7     = blockCodec{color.NRGBAModel, decompressBC7}
)

var fourCCCodecs = map[[4]byte]blockCodec{
	{'A', 'T', 'I', '1'}: codec3DcPlus,
	{'A', 'T', 'I', '2'}: codec3Dc,
	{'D', 'X', 'T', '1'}: codecDXT1,
	{'D', 'X', 'T', '5'}: codecDXT5,
}

var dxgiCodecs = map[dxgiFormat]blockCodec{
	dxgiFormatBC1UNorm: codecDXT1,
	dxgiFormatBC3UNorm: codecDXT5,
	dxgiFormatBC4UNorm: codec3DcPlus,
	dxgiFormatBC5UNorm: codec3Dc,
	dxgiFormatBC7UNorm: codecBC7,
}

var errDXT3Unsupported = errors.New("DXT3 compression unsupported")

func decodeInfo(r io.Reader) (imageInfo, error) {
	hdr, err := decodeHeader(r)
	if err != nil {
		return imageInfo{}, err
	}

	info := imageInfo{header: hdr}

	if hdr.pixelFormat.Flags&pixelFormatFlagRGB != 0 {
		info.colorModel = color.NRGBAModel
		info.decompress = decompressUncompressed
	} else if hdr.pixelFormat.Flags&pixelFormatFlagYUV != 0 {
		return imageInfo{}, errors.New("YUV compression unsupported")
	} else if hdr.pixelFormat.Flags&pixelFormatFlagLuminance != 0 {
		if hdr.pixelFormat.Flags&pixelFormatFlagAlphaPixels == 0 {
			if hdr.pixelFormat.GBitMask == 0 && hdr.pixelFormat.BBitMask == 0 {
				if hdr.pixelFormat.RGBBitCount > 8 {
					info.colorModel = color.Gray16Model
				} else {
					info.colorModel = color.GrayModel
				}
			} else {
				info.colorModel = color.NRGBAModel
			}
		} else {
			info.colorModel = color.NRGBAModel
		}
		info.decompress = decompressUncompressed
	} else if hdr.pixelFormat.Flags&pixelFormatFlagFourCC != 0 {
		fourCC := hdr.pixelFormat.FourCC
		switch {
		case fourCC == [4]byte{'D', 'X', 'T', '3'}:
			return imageInfo{}, errDXT3Unsupported
		case fourCC == [4]byte{'D', 'X', '1', '0'}:
			dx10, err := decodeDXT10Header(r)
			if err != nil {
				return imageInfo{}, err
			}
			info.dxt10Header = &dx10

			if dx10.ResourceDimension < d3d10ResourceDimensionTexture2D {
				return imageInfo{}, fmt.Errorf("unsupported DXT10 resource dimension: %v", dx10.ResourceDimension)
			}

			if format, ok := dxgiUncompressedFormats[dx10.dxgiFormat]; ok {
				info.colorModel = format.colorModel
				info.decompress = decompressUncompressedDXT10
			} else if codec, ok := dxgiCodecs[dx10.dxgiFormat]; ok {
				info.colorModel = codec.colorModel
				info.decompress = codec.decompress
			} else if dx10.dxgiFormat == dxgiFormatBC2UNorm {
				return imageInfo{}, errDXT3Unsupported
			} else if dx10.dxgiFormat == dxgiFormatBC7UNormSRGB {
				return imageInfo{}, errors.New("BC7 SRGB compression unsupported")
			} else {
				return imageInfo{}, fmt.Errorf("unsupported DXGI format: %v", dx10.dxgiFormat)
			}
		default:
			codec, ok := fourCCCodecs[fourCC]
			if !ok {
				return imageInfo{}, fmt.Errorf("unsupported compression format: unknown fourCC: %v", string(fourCC[:]))
			}
			info.colorModel = codec.colorModel
			info.decompress = codec.decompress
		}
	}

	return info, nil
}

// Decode decodes a DDS image from raw byte data, returning the first image (mip 0).
func Decode(data []byte) (image.Image, error) {
	r := bytes.NewReader(data)

	info, err := decodeInfo(r)
	if err != nil {
		return nil, err
	}

	width, height := int(info.header.Width), int(info.header.Height)
	if width < 1 || height < 1 {
		return nil, fmt.Errorf("invalid image dimensions %dx%d", width, height)
	}

	var buf []uint8
	var img image.Image
	switch info.colorModel {
	case color.GrayModel:
		newImg := image.NewGray(image.Rect(0, 0, width, height))
		buf = newImg.Pix
		img = newImg
	case color.Gray16Model:
		newImg := image.NewGray16(image.Rect(0, 0, width, height))
		buf = newImg.Pix
		img = newImg
	case color.NRGBAModel:
		newImg := image.NewNRGBA(image.Rect(0, 0, width, height))
		buf = newImg.Pix
		img = newImg
	case color.NRGBA64Model:
		newImg := image.NewNRGBA64(image.Rect(0, 0, width, height))
		buf = newImg.Pix
		img = newImg
	default:
		return nil, errors.New("invalid color model passed by info structure")
	}

	if err = info.decompress(buf, r, width, height, info); err != nil {
		return nil, fmt.Errorf("decompressing image: %w", err)
	}

	return img, nil
}
