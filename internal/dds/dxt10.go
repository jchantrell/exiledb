package dds

import (
	"encoding/binary"
	"fmt"
	"io"
)

type dxgiFormat uint32

const (
	dxgiFormatUnknown dxgiFormat = iota
	dxgiFormatR32G32B32A32Typeless
	dxgiFormatR32G32B32A32Float
	dxgiFormatR32G32B32A32UInt
	dxgiFormatR32G32B32A32SInt
	dxgiFormatR32G32B32Typeless
	dxgiFormatR32G32B32Float
	dxgiFormatR32G32B32UInt
	dxgiFormatR32G32B32SInt
	dxgiFormatR16G16B16A16Typeless
	dxgiFormatR16G16B16A16Float
	dxgiFormatR16G16B16A16UNorm
	dxgiFormatR16G16B16A16UInt
	dxgiFormatR16G16B16A16SNorm
	dxgiFormatR16G16B16A16SInt
	dxgiFormatR32G32Typeless
	dxgiFormatR32G32Float
	dxgiFormatR32G32UInt
	dxgiFormatR32G32SInt
	dxgiFormatR32G8X24Typeless
	dxgiFormatD32FLOATS8X24UInt
	dxgiFormatR32FLOATX8X24Typeless
	dxgiFormatX32TYPELESSG8X24UInt
	dxgiFormatR10G10B10A2Typeless
	dxgiFormatR10G10B10A2UNorm
	dxgiFormatR10G10B10A2UInt
	dxgiFormatR11G11B10Float
	dxgiFormatR8G8B8A8Typeless
	dxgiFormatR8G8B8A8UNorm
	dxgiFormatR8G8B8A8UNormSRGB
	dxgiFormatR8G8B8A8UInt
	dxgiFormatR8G8B8A8SNorm
	dxgiFormatR8G8B8A8SInt
	dxgiFormatR16G16Typeless
	dxgiFormatR16G16Float
	dxgiFormatR16G16UNorm
	dxgiFormatR16G16UInt
	dxgiFormatR16G16SNorm
	dxgiFormatR16G16SInt
	dxgiFormatR32Typeless
	dxgiFormatD32Float
	dxgiFormatR32Float
	dxgiFormatR32UInt
	dxgiFormatR32SInt
	dxgiFormatR24G8Typeless
	dxgiFormatD24UnormS8UInt
	dxgiFormatR24UnormX8Typeless
	dxgiFormatX24TypelessG8UInt
	dxgiFormatR8G8Typeless
	dxgiFormatR8G8UNorm
	dxgiFormatR8G8UInt
	dxgiFormatR8G8SNorm
	dxgiFormatR8G8SInt
	dxgiFormatR16Typeless
	dxgiFormatR16Float
	dxgiFormatD16UNorm
	dxgiFormatR16UNorm
	dxgiFormatR16UInt
	dxgiFormatR16SNorm
	dxgiFormatR16SInt
	dxgiFormatR8Typeless
	dxgiFormatR8UNorm
	dxgiFormatR8UInt
	dxgiFormatR8SNorm
	dxgiFormatR8SInt
	dxgiFormatA8UNorm
	dxgiFormatR1UNorm
	dxgiFormatR9G9B9E5SharedExp
	dxgiFormatR8G8B8G8UNorm
	dxgiFormatG8R8G8B8UNorm
	dxgiFormatBC1Typeless
	dxgiFormatBC1UNorm
	dxgiFormatBC1UNormSRGB
	dxgiFormatBC2Typeless
	dxgiFormatBC2UNorm
	dxgiFormatBC2UNormSRGB
	dxgiFormatBC3Typeless
	dxgiFormatBC3UNorm
	dxgiFormatBC3UNormSRGB
	dxgiFormatBC4Typeless
	dxgiFormatBC4UNorm
	dxgiFormatBC4SNorm
	dxgiFormatBC5Typeless
	dxgiFormatBC5UNorm
	dxgiFormatBC5SNorm
	dxgiFormatB5G6R5UNorm
	dxgiFormatB5G5R5A1UNorm
	dxgiFormatB8G8R8A8UNorm
	dxgiFormatB8G8R8X8UNorm
	dxgiFormatR10G10B10XRBiasA2UNorm
	dxgiFormatB8G8R8A8Typeless
	dxgiFormatB8G8R8A8UNormSRGB
	dxgiFormatB8G8R8X8Typeless
	dxgiFormatB8G8R8X8UNormSRGB
	dxgiFormatBC6HTypeless
	dxgiFormatBC6HUF16
	dxgiFormatBC6HSF16
	dxgiFormatBC7Typeless
	dxgiFormatBC7UNorm
	dxgiFormatBC7UNormSRGB
)

func (f dxgiFormat) String() string {
	switch f {
	case dxgiFormatUnknown:
		return "Unknown"
	case dxgiFormatR32G32B32A32Float:
		return "R32G32B32A32Float"
	case dxgiFormatR32G32B32Float:
		return "R32G32B32Float"
	case dxgiFormatR16G16B16A16Float:
		return "R16G16B16A16Float"
	case dxgiFormatR16G16B16A16UNorm:
		return "R16G16B16A16UNorm"
	case dxgiFormatR32G32Float:
		return "R32G32Float"
	case dxgiFormatR8G8B8A8UNorm:
		return "R8G8B8A8UNorm"
	case dxgiFormatR32Float:
		return "R32Float"
	case dxgiFormatR16UNorm:
		return "R16UNorm"
	case dxgiFormatR8UNorm:
		return "R8UNorm"
	case dxgiFormatB8G8R8A8UNorm:
		return "B8G8R8A8UNorm"
	case dxgiFormatBC1UNorm:
		return "BC1UNorm"
	case dxgiFormatBC2UNorm:
		return "BC2UNorm"
	case dxgiFormatBC3UNorm:
		return "BC3UNorm"
	case dxgiFormatBC4UNorm:
		return "BC4UNorm"
	case dxgiFormatBC5UNorm:
		return "BC5UNorm"
	case dxgiFormatBC7UNorm:
		return "BC7UNorm"
	case dxgiFormatBC7UNormSRGB:
		return "BC7UNormSRGB"
	default:
		return fmt.Sprintf("DXGIFormat(%d)", f)
	}
}

type d3d10ResourceDimension uint32

const (
	d3d10ResourceDimensionUnknown d3d10ResourceDimension = iota
	d3d10ResourceDimensionBuffer
	d3d10ResourceDimensionTexture1D
	d3d10ResourceDimensionTexture2D
	d3d10ResourceDimensionTexture3D
)

type d3d10ResourceMiscFlags uint32

const (
	d3d10ResourceMiscFlagTextureCube d3d10ResourceMiscFlags = 1 << 2
)

type alphaMode uint32

type dxt10Header struct {
	dxgiFormat        dxgiFormat
	ResourceDimension d3d10ResourceDimension
	MiscFlag          d3d10ResourceMiscFlags
	ArraySize         uint32
	MiscFlags2        alphaMode
}

func decodeDXT10Header(r io.Reader) (dxt10Header, error) {
	var dxt10Hdr dxt10Header
	if err := binary.Read(r, binary.LittleEndian, &dxt10Hdr); err != nil {
		return dxt10Header{}, err
	}
	return dxt10Hdr, nil
}
