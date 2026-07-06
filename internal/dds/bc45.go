package dds

import (
	"errors"
	"image/color"
	"io"
)

func decode3DcBlock(block []uint8) [8]uint8 {
	var c [8]uint8
	c[0], c[1] = block[0], block[1]

	mode := 4
	if c[0] > c[1] {
		mode = 6
	}
	for i := 0; i < mode; i++ {
		// Keep this float64 lerp exactly as-is: an integer or float32
		// reformulation can drift by 1 LSB on some inputs.
		c[i+2] = uint8(
			(float64((mode-i))*float64(c[0]) + float64(i+1)*float64(c[1])) /
				float64(mode+1))
	}
	if mode == 4 {
		c[6] = 0
		c[7] = 255
	}

	return c
}

func decompress3DcPlus(buf []uint8, r io.Reader, width, height int, info imageInfo) error {
	if info.colorModel != color.GrayModel {
		return errors.New("3Dc+ compression expects gray color model")
	}
	return decodeBlocks(buf, r, width, height, 8, 1, decode3DcPlusBlock)
}

func decode3DcPlusBlock(block []byte, texels *[16][4]uint8) error {
	c := decode3DcBlock(block)

	startBit := uint64(16)
	for t := 0; t < 16; t++ {
		texels[t][0] = c[getBits(block, &startBit, 3)]
	}
	return nil
}

func decompress3Dc(buf []uint8, r io.Reader, width, height int, info imageInfo) error {
	if info.colorModel != color.NRGBAModel {
		return errors.New("3Dc compression expects NRGBA color model")
	}
	return decodeBlocks(buf, r, width, height, 16, 4, decode3DcTwoChannelBlock)
}

func decode3DcTwoChannelBlock(block []byte, texels *[16][4]uint8) error {
	cR := decode3DcBlock(block[:8])
	cG := decode3DcBlock(block[8:])

	startBitR := uint64(16)
	startBitG := uint64(80)
	for t := 0; t < 16; t++ {
		r := cR[getBits(block, &startBitR, 3)]
		g := cG[getBits(block, &startBitG, 3)]
		texels[t] = [4]uint8{r, g, 0, 255}
	}
	return nil
}
