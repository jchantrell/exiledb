package dds

import (
	"encoding/binary"
	"errors"
	"image/color"
	"io"
)

func avgU8(xs ...uint8) uint8 {
	var sum int
	for _, x := range xs {
		sum += int(x)
	}
	return uint8(sum / len(xs))
}

func calculateDXTColors(c0 uint16, c1 uint16, ignoreAlpha bool) (r [4]uint8, g [4]uint8, b [4]uint8, a [4]uint8) {
	r[0], g[0], b[0] = colorR5G6B5ToRGB(c0)
	r[1], g[1], b[1] = colorR5G6B5ToRGB(c1)

	if ignoreAlpha || c0 > c1 {
		r[2] = avgU8(r[0], r[0], r[1])
		g[2] = avgU8(g[0], g[0], g[1])
		b[2] = avgU8(b[0], b[0], b[1])

		r[3] = avgU8(r[0], r[1], r[1])
		g[3] = avgU8(g[0], g[1], g[1])
		b[3] = avgU8(b[0], b[1], b[1])

		a = [4]uint8{255, 255, 255, 255}
	} else {
		r[2] = avgU8(r[0], r[1])
		g[2] = avgU8(g[0], g[1])
		b[2] = avgU8(b[0], b[1])

		a = [4]uint8{255, 255, 255, 0}
	}
	return
}

func DecompressDXT1(buf []uint8, r io.Reader, width, height int, info Info) error {
	if info.ColorModel != color.NRGBAModel {
		return errors.New("DXT1 compression expects NRGBA color model")
	}
	return decodeBlocks(buf, r, width, height, 8, 4, decodeDXT1Block)
}

func decodeDXT1Block(block []byte, texels *[16][4]uint8) error {
	c0 := binary.LittleEndian.Uint16(block[:2])
	c1 := binary.LittleEndian.Uint16(block[2:4])
	bits := binary.LittleEndian.Uint32(block[4:8])

	cR, cG, cB, cA := calculateDXTColors(c0, c1, false)

	for t := 0; t < 16; t++ {
		code := (bits >> (t * 2)) & 0x03
		texels[t] = [4]uint8{cR[code], cG[code], cB[code], cA[code]}
	}
	return nil
}

func DecompressDXT5(buf []uint8, r io.Reader, width, height int, info Info) error {
	if info.ColorModel != color.NRGBAModel {
		return errors.New("DXT5 compression expects NRGBA color model")
	}
	return decodeBlocks(buf, r, width, height, 16, 4, decodeDXT5Block)
}

func decodeDXT5Block(block []byte, texels *[16][4]uint8) error {
	a0, a1 := block[0], block[1]
	alphaBits := uint64(binary.LittleEndian.Uint32(block[2:6]))
	alphaBits |= uint64(binary.LittleEndian.Uint16(block[6:8])) << 32
	c0, c1 := binary.LittleEndian.Uint16(block[8:10]), binary.LittleEndian.Uint16(block[10:12])
	bits := binary.LittleEndian.Uint32(block[12:16])

	cR, cG, cB, _ := calculateDXTColors(c0, c1, true)

	for t := 0; t < 16; t++ {
		code := (bits >> (t * 2)) & 0x3

		alphaCode := (alphaBits >> (3 * t)) & 0x7
		var alpha uint8
		if alphaCode == 0 {
			alpha = a0
		} else if alphaCode == 1 {
			alpha = a1
		} else if a0 > a1 {
			alpha = uint8(((8-alphaCode)*uint64(a0) + (alphaCode-1)*uint64(a1)) / 7)
		} else if alphaCode == 6 {
			alpha = 0
		} else if alphaCode == 7 {
			alpha = 255
		} else {
			alpha = uint8(((6-alphaCode)*uint64(a0) + (alphaCode)*uint64(a1)) / 5)
		}

		texels[t] = [4]uint8{cR[code], cG[code], cB[code], alpha}
	}
	return nil
}
