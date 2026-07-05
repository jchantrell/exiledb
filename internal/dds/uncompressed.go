package dds

import (
	"encoding/binary"
	"errors"
	"fmt"
	"image/color"
	"io"
	"math"
	"math/bits"

	"github.com/x448/float16"
)

func colorModelStride(m color.Model) (int, error) {
	switch m {
	case color.GrayModel:
		return 1, nil
	case color.Gray16Model:
		return 2, nil
	case color.NRGBAModel:
		return 4, nil
	case color.NRGBA64Model:
		return 8, nil
	default:
		return 0, errors.New("uncompressed image: unexpected color model")
	}
}

func DecompressUncompressed(buf []uint8, r io.Reader, width, height int, info Info) ([]uint8, error) {
	bitMasks := [4]uint32{info.Header.PixelFormat.RBitMask, info.Header.PixelFormat.GBitMask, info.Header.PixelFormat.BBitMask, info.Header.PixelFormat.ABitMask}
	var bitMaskTZs [4]int
	for i := range bitMasks {
		bitMaskTZs[i] = bits.TrailingZeros32(bitMasks[i])
	}
	var bitMaskBits [4]int
	for i := range bitMasks {
		bitMaskBits[i] = bits.Len32(bitMasks[i] >> bitMaskTZs[i])
	}

	if info.Header.PixelFormat.RGBBitCount%8 != 0 {
		return nil, fmt.Errorf("invalid RGB bit count: %v (must be multiple of 8)", info.Header.PixelFormat.RGBBitCount)
	}
	byteCount := info.Header.PixelFormat.RGBBitCount / 8
	if byteCount > 4 {
		return nil, fmt.Errorf("invalid RGB bit count: %v (must be at most 32)", info.Header.PixelFormat.RGBBitCount)
	}

	stride, err := colorModelStride(info.ColorModel)
	if err != nil {
		return nil, err
	}

	// Validate the channel layout once: channel i in RGBA order writes to
	// slot idx+i (8-bit) or idx+2*i (16-bit), so the furthest slot end must
	// fit within the color model's stride.
	pixelEnd := 0
	for i := range bitMasks {
		var end int
		switch {
		case bitMaskBits[i] == 0:
			continue
		case bitMaskBits[i] <= 8:
			end = i + 1
		case bitMaskBits[i] == 16:
			end = 2*i + 2
		default:
			return nil, fmt.Errorf("unsupported number of bits: %v", bitMaskBits[i])
		}
		if end > pixelEnd {
			pixelEnd = end
		}
	}
	if pixelEnd > stride {
		return nil, fmt.Errorf("pixel format masks write up to byte %d per pixel, color model stride is %d", pixelEnd, stride)
	}

	fillAlpha := info.Header.PixelFormat.Flags&PixelFormatFlagAlphaPixels == 0

	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			idx := stride * (y*width + x)

			if fillAlpha {
				if info.ColorModel == color.NRGBAModel {
					buf[idx+3] = 0xff
				} else if info.ColorModel == color.NRGBA64Model {
					binary.BigEndian.PutUint16(buf[idx+6:], 0xffff)
				}
			}

			var data [4]uint8
			if _, err := io.ReadFull(r, data[:byteCount]); err != nil {
				return nil, err
			}
			dataU32 := binary.LittleEndian.Uint32(data[:])

			for i := range bitMasks {
				if bitMaskBits[i] == 0 {
					continue
				}
				c := (dataU32 & bitMasks[i]) >> bitMaskTZs[i]
				if bitMaskBits[i] <= 8 {
					var v uint8
					switch bitMaskBits[i] {
					case 1:
						v = mapBits1To8(uint16(c))
					case 2:
						v = mapBits2To8(uint16(c))
					case 3:
						v = mapBits3To8(uint16(c))
					case 4:
						v = mapBits4To8(uint16(c))
					case 5:
						v = mapBits5To8(uint16(c))
					case 6:
						v = mapBits6To8(uint16(c))
					case 7:
						v = mapBits7To8(uint16(c))
					case 8:
						v = uint8(c)
					}
					buf[idx+i] = v
				} else {
					binary.BigEndian.PutUint16(buf[idx+2*i:], uint16(c))
				}
			}
		}
	}
	return buf, nil
}

func DecompressUncompressedDXT10(buf []uint8, r io.Reader, width, height int, info Info) ([]uint8, error) {
	if info.DXT10Header == nil {
		return nil, errors.New("uncompressed DXT 10: expected DXT10 header")
	}

	// srcPixelBytes is the size of one source pixel; translateRow decodes a
	// full row of source pixels into buf starting at idx.
	var srcPixelBytes int
	var translateRow func(idx int, src []byte)
	switch info.DXT10Header.DXGIFormat {
	case DXGIFormatR32G32B32A32Float:
		if info.ColorModel != color.NRGBA64Model {
			return nil, errors.New("expected NRGBA64 model for R32G32B32A32Float")
		}
		srcPixelBytes = 16
		translateRow = func(idx int, src []byte) {
			for o := 0; o < len(src); o += 4 {
				v := math.Float32frombits(binary.LittleEndian.Uint32(src[o:]))
				binary.BigEndian.PutUint16(buf[idx:], uint16(v*0xffff))
				idx += 2
			}
		}
	case DXGIFormatR16G16B16A16Float:
		if info.ColorModel != color.NRGBA64Model {
			return nil, errors.New("expected NRGBA64 model for R16G16B16A16Float")
		}
		srcPixelBytes = 8
		translateRow = func(idx int, src []byte) {
			for o := 0; o < len(src); o += 2 {
				v := float16.Frombits(binary.LittleEndian.Uint16(src[o:]))
				binary.BigEndian.PutUint16(buf[idx:], uint16(v.Float32()*0xffff))
				idx += 2
			}
		}
	case DXGIFormatR32Float:
		if info.ColorModel != color.Gray16Model {
			return nil, errors.New("expected Gray16 model for R32Float")
		}
		srcPixelBytes = 4
		translateRow = func(idx int, src []byte) {
			for o := 0; o < len(src); o += 4 {
				v := math.Float32frombits(binary.LittleEndian.Uint32(src[o:]))
				binary.BigEndian.PutUint16(buf[idx:], uint16(v*0xffff))
				idx += 2
			}
		}
	case DXGIFormatR8G8B8A8UNorm:
		if info.ColorModel != color.NRGBAModel {
			return nil, errors.New("expected NRGBA model for R8G8B8A8UNorm")
		}
		srcPixelBytes = 4
		translateRow = func(idx int, src []byte) {
			copy(buf[idx:], src)
		}
	case DXGIFormatB8G8R8A8UNorm:
		if info.ColorModel != color.NRGBAModel {
			return nil, errors.New("expected NRGBA model for B8G8R8A8UNorm")
		}
		srcPixelBytes = 4
		translateRow = func(idx int, src []byte) {
			for o := 0; o < len(src); o += 4 {
				buf[idx], buf[idx+1], buf[idx+2], buf[idx+3] = src[o+2], src[o+1], src[o], src[o+3]
				idx += 4
			}
		}
	case DXGIFormatR16UNorm:
		if info.ColorModel != color.Gray16Model {
			return nil, errors.New("expected Gray16 model for R16UNorm")
		}
		srcPixelBytes = 2
		translateRow = func(idx int, src []byte) {
			for o := 0; o < len(src); o += 2 {
				binary.BigEndian.PutUint16(buf[idx:], binary.LittleEndian.Uint16(src[o:]))
				idx += 2
			}
		}
	case DXGIFormatR8UNorm:
		if info.ColorModel != color.GrayModel {
			return nil, errors.New("expected Gray model for R8UNorm")
		}
		srcPixelBytes = 1
		translateRow = func(idx int, src []byte) {
			copy(buf[idx:], src)
		}
	default:
		return nil, fmt.Errorf("uncompressed image: unsupported DXGI format: %v", info.DXT10Header.DXGIFormat)
	}

	stride, err := colorModelStride(info.ColorModel)
	if err != nil {
		return nil, err
	}
	row := make([]byte, width*srcPixelBytes)
	for y := 0; y < height; y++ {
		if _, err := io.ReadFull(r, row); err != nil {
			return nil, err
		}
		translateRow(stride*y*width, row)
	}
	return nil, nil
}
