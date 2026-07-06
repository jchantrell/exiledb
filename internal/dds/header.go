package dds

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type capsFlags uint32

type caps2Flags uint32

type pixelFormatFlags uint32

const (
	pixelFormatFlagAlphaPixels pixelFormatFlags = 1 << 0
	pixelFormatFlagAlpha       pixelFormatFlags = 1 << 1
	pixelFormatFlagFourCC      pixelFormatFlags = 1 << 2
	pixelFormatFlagRGB         pixelFormatFlags = 1 << 6
	pixelFormatFlagYUV         pixelFormatFlags = 1 << 9
	pixelFormatFlagLuminance   pixelFormatFlags = 1 << 17
)

type pixelFormat struct {
	Size        uint32
	Flags       pixelFormatFlags
	FourCC      [4]uint8
	RGBBitCount uint32
	RBitMask    uint32
	GBitMask    uint32
	BBitMask    uint32
	ABitMask    uint32
}

type headerFlags uint32

const (
	headerFlagCaps        headerFlags = 1 << 0
	headerFlagHeight      headerFlags = 1 << 1
	headerFlagWidth       headerFlags = 1 << 2
	headerFlagPitch       headerFlags = 1 << 3
	headerFlagPixelFormat headerFlags = 1 << 12
	headerFlagMipMapCount headerFlags = 1 << 17
	headerFlagLinearsize  headerFlags = 1 << 19
	headerFlagDepth       headerFlags = 1 << 23
)

type header struct {
	Size              uint32
	Flags             headerFlags
	Height            uint32
	Width             uint32
	PitchOrLinearSize uint32
	Depth             uint32
	MipMapCount       uint32
	Reserved          [11]uint32
	pixelFormat       pixelFormat
	caps              capsFlags
	caps2             caps2Flags
	caps3             uint32
	caps4             uint32
	Reserved2         uint32
}

func decodeHeader(r io.Reader) (header, error) {
	var magicNum [4]uint8
	if err := binary.Read(r, binary.LittleEndian, magicNum[:]); err != nil {
		return header{}, err
	}
	if magicNum != [4]uint8{'D', 'D', 'S', ' '} {
		return header{}, errors.New("invalid magic number")
	}

	var hdr header
	if err := binary.Read(r, binary.LittleEndian, &hdr); err != nil {
		return header{}, err
	}

	if hdr.Size != 0x7c {
		return header{}, fmt.Errorf("invalid header size: %v", hdr.Size)
	}

	if hdr.Flags&headerFlagCaps == 0 ||
		hdr.Flags&headerFlagWidth == 0 ||
		hdr.Flags&headerFlagHeight == 0 ||
		hdr.Flags&headerFlagPixelFormat == 0 {
		return header{}, errors.New("required header flags missing (required: Caps | Width | Height | PixelFormat)")
	}

	if hdr.pixelFormat.Size != 0x20 {
		return header{}, fmt.Errorf("invalid pixel format header size: %v", hdr.pixelFormat.Size)
	}

	return hdr, nil
}
