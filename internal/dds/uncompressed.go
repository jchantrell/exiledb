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

func decompressUncompressed(buf []uint8, r io.Reader, width, height int, info imageInfo) error {
	bitMasks := [4]uint32{info.header.pixelFormat.RBitMask, info.header.pixelFormat.GBitMask, info.header.pixelFormat.BBitMask, info.header.pixelFormat.ABitMask}
	var bitMaskTZs [4]int
	for i := range bitMasks {
		bitMaskTZs[i] = bits.TrailingZeros32(bitMasks[i])
	}
	var bitMaskBits [4]int
	for i := range bitMasks {
		bitMaskBits[i] = bits.Len32(bitMasks[i] >> bitMaskTZs[i])
	}

	if info.header.pixelFormat.RGBBitCount%8 != 0 {
		return fmt.Errorf("invalid RGB bit count: %v (must be multiple of 8)", info.header.pixelFormat.RGBBitCount)
	}
	byteCount := info.header.pixelFormat.RGBBitCount / 8
	if byteCount > 4 {
		return fmt.Errorf("invalid RGB bit count: %v (must be at most 32)", info.header.pixelFormat.RGBBitCount)
	}

	stride, err := colorModelStride(info.colorModel)
	if err != nil {
		return err
	}

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
			return fmt.Errorf("unsupported number of bits: %v", bitMaskBits[i])
		}
		if end > pixelEnd {
			pixelEnd = end
		}
	}
	if pixelEnd > stride {
		return fmt.Errorf("pixel format masks write up to byte %d per pixel, color model stride is %d", pixelEnd, stride)
	}

	fillAlpha := info.header.pixelFormat.Flags&pixelFormatFlagAlphaPixels == 0

	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			idx := stride * (y*width + x)

			if fillAlpha {
				if info.colorModel == color.NRGBAModel {
					buf[idx+3] = 0xff
				} else if info.colorModel == color.NRGBA64Model {
					binary.BigEndian.PutUint16(buf[idx+6:], 0xffff)
				}
			}

			var data [4]uint8
			if _, err := io.ReadFull(r, data[:byteCount]); err != nil {
				return err
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
	return nil
}

type dxgiUncompressedFormat struct {
	colorModel    color.Model
	srcPixelBytes int
	translateRow  func(dst, src []byte)
}

var dxgiUncompressedFormats = map[dxgiFormat]dxgiUncompressedFormat{
	dxgiFormatR32G32B32A32Float: {color.NRGBA64Model, 16, translateFloat32Row},
	dxgiFormatR16G16B16A16Float: {color.NRGBA64Model, 8, translateFloat16Row},
	dxgiFormatR16G16B16A16UNorm: {color.NRGBA64Model, 8, translateUint16Row},
	dxgiFormatR32Float:          {color.Gray16Model, 4, translateFloat32Row},
	dxgiFormatR16UNorm:          {color.Gray16Model, 2, translateUint16Row},
	dxgiFormatR8G8B8A8UNorm:     {color.NRGBAModel, 4, translateCopyRow},
	dxgiFormatB8G8R8A8UNorm:     {color.NRGBAModel, 4, translateBGRARow},
	dxgiFormatR8UNorm:           {color.GrayModel, 1, translateCopyRow},
}

func translateFloat32Row(dst, src []byte) {
	for o := 0; o < len(src); o += 4 {
		v := math.Float32frombits(binary.LittleEndian.Uint32(src[o:]))
		binary.BigEndian.PutUint16(dst, uint16(v*0xffff))
		dst = dst[2:]
	}
}

func translateFloat16Row(dst, src []byte) {
	for o := 0; o < len(src); o += 2 {
		v := float16.Frombits(binary.LittleEndian.Uint16(src[o:]))
		binary.BigEndian.PutUint16(dst, uint16(v.Float32()*0xffff))
		dst = dst[2:]
	}
}

func translateUint16Row(dst, src []byte) {
	for o := 0; o < len(src); o += 2 {
		binary.BigEndian.PutUint16(dst, binary.LittleEndian.Uint16(src[o:]))
		dst = dst[2:]
	}
}

func translateCopyRow(dst, src []byte) {
	copy(dst, src)
}

func translateBGRARow(dst, src []byte) {
	for o := 0; o < len(src); o += 4 {
		dst[0], dst[1], dst[2], dst[3] = src[o+2], src[o+1], src[o], src[o+3]
		dst = dst[4:]
	}
}

func decompressUncompressedDXT10(buf []uint8, r io.Reader, width, height int, info imageInfo) error {
	if info.dxt10Header == nil {
		return errors.New("uncompressed DXT 10: expected DXT10 header")
	}

	format, ok := dxgiUncompressedFormats[info.dxt10Header.dxgiFormat]
	if !ok {
		return fmt.Errorf("uncompressed image: unsupported DXGI format: %v", info.dxt10Header.dxgiFormat)
	}
	if info.colorModel != format.colorModel {
		return fmt.Errorf("unexpected color model for %v", info.dxt10Header.dxgiFormat)
	}

	stride, err := colorModelStride(info.colorModel)
	if err != nil {
		return err
	}
	row := make([]byte, width*format.srcPixelBytes)
	for y := 0; y < height; y++ {
		if _, err := io.ReadFull(r, row); err != nil {
			return err
		}
		format.translateRow(buf[stride*y*width:], row)
	}
	return nil
}
