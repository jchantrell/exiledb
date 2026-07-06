package dds

import "io"

type decompressFunc func(buf []uint8, r io.Reader, width, height int, info imageInfo) error

// blockDecoder decodes one compressed block into 16 texels in row-major
// order: texels[j*4+i] holds texel (i, j) of the 4x4 block. Each texel
// carries up to four channel bytes; the driver's stride decides how many
// are written out.
type blockDecoder func(block []byte, texels *[16][4]uint8) error

// decodeBlocks drives a block codec across the image: it reads blockSize
// bytes per 4x4 block, decodes them into a scratch texel array, and blits
// stride bytes per texel into buf, clipping texels of blocks that overhang
// the right or bottom edge.
func decodeBlocks(buf []uint8, r io.Reader, width, height, blockSize, stride int, dec blockDecoder) error {
	block := make([]byte, blockSize)
	var texels [16][4]uint8
	for y := 0; y < height; y += 4 {
		for x := 0; x < width; x += 4 {
			if _, err := io.ReadFull(r, block); err != nil {
				return err
			}
			if err := dec(block, &texels); err != nil {
				return err
			}
			for j := 0; j < 4; j++ {
				for i := 0; i < 4; i++ {
					if x+i >= width || y+j >= height {
						continue
					}
					idx := stride * ((y+j)*width + (x + i))
					copy(buf[idx:idx+stride], texels[j*4+i][:stride])
				}
			}
		}
	}
	return nil
}
