package dds

import (
	"bytes"
	"errors"
	"fmt"
	"image"
	"image/color"
	"io"
)

type Info struct {
	Header      Header
	DXT10Header *DXT10Header
	Decompress  DecompressFunc
	ColorModel  color.Model
	NumMipMaps  int
	NumImages   int
}

type blockCodec struct {
	colorModel color.Model
	decompress DecompressFunc
}

var (
	codecDXT1    = blockCodec{color.NRGBAModel, DecompressDXT1}
	codecDXT5    = blockCodec{color.NRGBAModel, DecompressDXT5}
	codec3DcPlus = blockCodec{color.GrayModel, Decompress3DcPlus}
	codec3Dc     = blockCodec{color.NRGBAModel, Decompress3Dc}
	codecBC7     = blockCodec{color.NRGBAModel, DecompressBC7}
)

var fourCCCodecs = map[[4]byte]blockCodec{
	{'A', 'T', 'I', '1'}: codec3DcPlus,
	{'A', 'T', 'I', '2'}: codec3Dc,
	{'D', 'X', 'T', '1'}: codecDXT1,
	{'D', 'X', 'T', '5'}: codecDXT5,
}

var dxgiCodecs = map[DXGIFormat]blockCodec{
	DXGIFormatBC1UNorm: codecDXT1,
	DXGIFormatBC3UNorm: codecDXT5,
	DXGIFormatBC4UNorm: codec3DcPlus,
	DXGIFormatBC5UNorm: codec3Dc,
	DXGIFormatBC7UNorm: codecBC7,
}

var errDXT3Unsupported = errors.New("DXT3 compression unsupported")

func DecodeInfo(r io.Reader) (Info, error) {
	hdr, err := DecodeHeader(r)
	if err != nil {
		return Info{}, err
	}

	info := Info{
		Header:     hdr,
		NumMipMaps: 1,
	}

	cubemap := hdr.Caps2&Caps2Cubemap != 0
	volume := hdr.Caps2&Caps2Volume != 0 && hdr.Depth > 0

	if hdr.PixelFormat.Flags&PixelFormatFlagRGB != 0 {
		info.ColorModel = color.NRGBAModel
		info.Decompress = DecompressUncompressed
	} else if hdr.PixelFormat.Flags&PixelFormatFlagYUV != 0 {
		return Info{}, errors.New("YUV compression unsupported")
	} else if hdr.PixelFormat.Flags&PixelFormatFlagLuminance != 0 {
		if hdr.PixelFormat.Flags&PixelFormatFlagAlphaPixels == 0 {
			if hdr.PixelFormat.GBitMask == 0 && hdr.PixelFormat.BBitMask == 0 {
				if hdr.PixelFormat.RGBBitCount > 8 {
					info.ColorModel = color.Gray16Model
				} else {
					info.ColorModel = color.GrayModel
				}
			} else {
				info.ColorModel = color.NRGBAModel
			}
		} else {
			info.ColorModel = color.NRGBAModel
		}
		info.Decompress = DecompressUncompressed
	} else if hdr.PixelFormat.Flags&PixelFormatFlagFourCC != 0 {
		fourCC := hdr.PixelFormat.FourCC
		switch {
		case fourCC == [4]byte{'D', 'X', 'T', '3'}:
			return Info{}, errDXT3Unsupported
		case fourCC == [4]byte{'D', 'X', '1', '0'}:
			dx10, err := DecodeDXT10Header(r)
			if err != nil {
				return Info{}, err
			}
			info.DXT10Header = &dx10

			if dx10.ResourceDimension < D3D10ResourceDimensionTexture2D {
				return Info{}, fmt.Errorf("unsupported DXT10 resource dimension: %v", dx10.ResourceDimension)
			}

			if format, ok := dxgiUncompressedFormats[dx10.DXGIFormat]; ok {
				info.ColorModel = format.colorModel
				info.Decompress = DecompressUncompressedDXT10
			} else if codec, ok := dxgiCodecs[dx10.DXGIFormat]; ok {
				info.ColorModel = codec.colorModel
				info.Decompress = codec.decompress
			} else if dx10.DXGIFormat == DXGIFormatBC2UNorm {
				return Info{}, errDXT3Unsupported
			} else if dx10.DXGIFormat == DXGIFormatBC7UNormSRGB {
				return Info{}, errors.New("BC7 SRGB compression unsupported")
			} else {
				return Info{}, fmt.Errorf("unsupported DXGI format: %v", dx10.DXGIFormat)
			}

			if dx10.MiscFlag&D3D10ResourceMiscFlagTextureCube != 0 {
				cubemap = true
			}
		default:
			codec, ok := fourCCCodecs[fourCC]
			if !ok {
				return Info{}, fmt.Errorf("unsupported compression format: unknown fourCC: %v", string(fourCC[:]))
			}
			info.ColorModel = codec.colorModel
			info.Decompress = codec.decompress
		}
	}

	info.NumImages = 1

	if info.DXT10Header != nil {
		info.NumImages = int(info.DXT10Header.ArraySize)
	}

	if cubemap {
		info.NumImages = 0
		if hdr.Caps2&Caps2CubemapPlusX != 0 {
			info.NumImages++
		}
		if hdr.Caps2&Caps2CubemapMinusX != 0 {
			info.NumImages++
		}
		if hdr.Caps2&Caps2CubemapPlusY != 0 {
			info.NumImages++
		}
		if hdr.Caps2&Caps2CubemapMinusY != 0 {
			info.NumImages++
		}
		if hdr.Caps2&Caps2CubemapPlusZ != 0 {
			info.NumImages++
		}
		if hdr.Caps2&Caps2CubemapMinusZ != 0 {
			info.NumImages++
		}
	}

	if volume {
		info.NumImages = int(hdr.Depth)
	}

	if info.NumImages == 0 {
		return Info{}, errors.New("invalid image header: no images")
	}

	if hdr.Caps&CapsMipMap != 0 &&
		(hdr.Caps&CapsTexture != 0 || hdr.Caps2&Caps2Cubemap != 0) {
		info.NumMipMaps = int(hdr.MipMapCount)
	}

	if info.NumMipMaps == 0 {
		return Info{}, errors.New("invalid image header: base image mipmap (mip 0) missing")
	}

	return info, nil
}

// Decode decodes a DDS image from raw byte data, returning the first image (mip 0).
func Decode(data []byte) (image.Image, error) {
	r := bytes.NewReader(data)

	info, err := DecodeInfo(r)
	if err != nil {
		return nil, err
	}

	width, height := int(info.Header.Width), int(info.Header.Height)
	if width < 1 || height < 1 {
		return nil, fmt.Errorf("invalid image dimensions %dx%d", width, height)
	}

	var buf []uint8
	var img image.Image
	switch info.ColorModel {
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

	if err = info.Decompress(buf, r, width, height, info); err != nil {
		return nil, fmt.Errorf("decompressing image: %w", err)
	}

	return img, nil
}
