package dds

import (
	"errors"
	"fmt"
	"image/color"
	"io"
)

// bc7ModeInfo encodes the full per-mode block layout. Derived facts:
// a mode has partitions iff PartitionBits > 0, endpoint p-bits iff
// NumPBits > 0 (shared per subset when NumPBits == NumSubsets, per
// endpoint otherwise), alpha endpoints iff AlphaPrecision > 0, and a
// second index set iff Index2Precision > 0.
var bc7ModeInfo = [8]struct {
	PartitionBits      uint8
	NumSubsets         uint8
	RotationBits       uint8
	IndexSelectionBits uint8
	ColorPrecision     uint8
	AlphaPrecision     uint8
	NumPBits           uint8
	IndexPrecision     uint8
	Index2Precision    uint8
}{
	{4, 3, 0, 0, 4, 0, 6, 3, 0},
	{6, 2, 0, 0, 6, 0, 2, 3, 0},
	{6, 3, 0, 0, 5, 0, 0, 2, 0},
	{6, 2, 0, 0, 7, 0, 4, 2, 0},
	{0, 1, 2, 1, 5, 6, 0, 2, 3},
	{0, 1, 2, 0, 7, 8, 0, 2, 2},
	{0, 1, 0, 0, 7, 7, 2, 4, 0},
	{6, 2, 0, 0, 5, 5, 4, 2, 0},
}

var bc7PartitionTable = [2][64][16]uint8{
	{ // Partition set for 2 subsets
		{0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1},
		{0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1},
		{0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1},
		{0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 1, 1, 1},
		{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 1, 1},
		{0, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1},
		{0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1},
		{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 1, 1},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1},
		{0, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		{0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 1},
		{0, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1},
		{0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1},
		{0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1, 1},
		{0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0},
		{0, 1, 1, 1, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0},
		{0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0},
		{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0},
		{0, 1, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 0, 1},
		{0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0},
		{0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0},
		{0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0},
		{0, 0, 1, 1, 0, 1, 1, 0, 0, 1, 1, 0, 1, 1, 0, 0},
		{0, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0},
		{0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0},
		{0, 1, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0},
		{0, 0, 1, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0, 0},
		{0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1},
		{0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1},
		{0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0},
		{0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0},
		{0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0},
		{0, 1, 0, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0},
		{0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1},
		{0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0, 1},
		{0, 1, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1, 1, 0},
		{0, 0, 0, 1, 0, 0, 1, 1, 1, 1, 0, 0, 1, 0, 0, 0},
		{0, 0, 1, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 1, 0, 0},
		{0, 0, 1, 1, 1, 0, 1, 1, 1, 1, 0, 1, 1, 1, 0, 0},
		{0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0},
		{0, 0, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1},
		{0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1},
		{0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0},
		{0, 1, 0, 0, 1, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0},
		{0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 1, 0},
		{0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 1, 0, 0},
		{0, 1, 1, 0, 1, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 1},
		{0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 0, 1, 0, 0, 1},
		{0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 1, 1, 1, 0, 0},
		{0, 0, 1, 1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 0},
		{0, 1, 1, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 0, 0, 1},
		{0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0, 0, 1},
		{0, 1, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1},
		{0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 1, 1, 1},
		{0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1},
		{0, 0, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0},
		{0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 1, 0, 1, 1, 1, 0},
		{0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, 1, 1, 1},
	},

	{ // Partition set for 3 subsets
		{0, 0, 1, 1, 0, 0, 1, 1, 0, 2, 2, 1, 2, 2, 2, 2},
		{0, 0, 0, 1, 0, 0, 1, 1, 2, 2, 1, 1, 2, 2, 2, 1},
		{0, 0, 0, 0, 2, 0, 0, 1, 2, 2, 1, 1, 2, 2, 1, 1},
		{0, 2, 2, 2, 0, 0, 2, 2, 0, 0, 1, 1, 0, 1, 1, 1},
		{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 1, 1, 2, 2},
		{0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 2, 2, 0, 0, 2, 2},
		{0, 0, 2, 2, 0, 0, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1},
		{0, 0, 1, 1, 0, 0, 1, 1, 2, 2, 1, 1, 2, 2, 1, 1},
		{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2},
		{0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2},
		{0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2},
		{0, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1, 2},
		{0, 1, 1, 2, 0, 1, 1, 2, 0, 1, 1, 2, 0, 1, 1, 2},
		{0, 1, 2, 2, 0, 1, 2, 2, 0, 1, 2, 2, 0, 1, 2, 2},
		{0, 0, 1, 1, 0, 1, 1, 2, 1, 1, 2, 2, 1, 2, 2, 2},
		{0, 0, 1, 1, 2, 0, 0, 1, 2, 2, 0, 0, 2, 2, 2, 0},
		{0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 1, 2, 1, 1, 2, 2},
		{0, 1, 1, 1, 0, 0, 1, 1, 2, 0, 0, 1, 2, 2, 0, 0},
		{0, 0, 0, 0, 1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 2, 2},
		{0, 0, 2, 2, 0, 0, 2, 2, 0, 0, 2, 2, 1, 1, 1, 1},
		{0, 1, 1, 1, 0, 1, 1, 1, 0, 2, 2, 2, 0, 2, 2, 2},
		{0, 0, 0, 1, 0, 0, 0, 1, 2, 2, 2, 1, 2, 2, 2, 1},
		{0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 2, 2, 0, 1, 2, 2},
		{0, 0, 0, 0, 1, 1, 0, 0, 2, 2, 1, 0, 2, 2, 1, 0},
		{0, 1, 2, 2, 0, 1, 2, 2, 0, 0, 1, 1, 0, 0, 0, 0},
		{0, 0, 1, 2, 0, 0, 1, 2, 1, 1, 2, 2, 2, 2, 2, 2},
		{0, 1, 1, 0, 1, 2, 2, 1, 1, 2, 2, 1, 0, 1, 1, 0},
		{0, 0, 0, 0, 0, 1, 1, 0, 1, 2, 2, 1, 1, 2, 2, 1},
		{0, 0, 2, 2, 1, 1, 0, 2, 1, 1, 0, 2, 0, 0, 2, 2},
		{0, 1, 1, 0, 0, 1, 1, 0, 2, 0, 0, 2, 2, 2, 2, 2},
		{0, 0, 1, 1, 0, 1, 2, 2, 0, 1, 2, 2, 0, 0, 1, 1},
		{0, 0, 0, 0, 2, 0, 0, 0, 2, 2, 1, 1, 2, 2, 2, 1},
		{0, 0, 0, 0, 0, 0, 0, 2, 1, 1, 2, 2, 1, 2, 2, 2},
		{0, 2, 2, 2, 0, 0, 2, 2, 0, 0, 1, 2, 0, 0, 1, 1},
		{0, 0, 1, 1, 0, 0, 1, 2, 0, 0, 2, 2, 0, 2, 2, 2},
		{0, 1, 2, 0, 0, 1, 2, 0, 0, 1, 2, 0, 0, 1, 2, 0},
		{0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 0, 0, 0, 0},
		{0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0},
		{0, 1, 2, 0, 2, 0, 1, 2, 1, 2, 0, 1, 0, 1, 2, 0},
		{0, 0, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2, 0, 0, 1, 1},
		{0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 0, 0, 0, 0, 1, 1},
		{0, 1, 0, 1, 0, 1, 0, 1, 2, 2, 2, 2, 2, 2, 2, 2},
		{0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 2, 1, 2, 1, 2, 1},
		{0, 0, 2, 2, 1, 1, 2, 2, 0, 0, 2, 2, 1, 1, 2, 2},
		{0, 0, 2, 2, 0, 0, 1, 1, 0, 0, 2, 2, 0, 0, 1, 1},
		{0, 2, 2, 0, 1, 2, 2, 1, 0, 2, 2, 0, 1, 2, 2, 1},
		{0, 1, 0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 0, 1, 0, 1},
		{0, 0, 0, 0, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1},
		{0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 2, 2, 2, 2},
		{0, 2, 2, 2, 0, 1, 1, 1, 0, 2, 2, 2, 0, 1, 1, 1},
		{0, 0, 0, 2, 1, 1, 1, 2, 0, 0, 0, 2, 1, 1, 1, 2},
		{0, 0, 0, 0, 2, 1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 2},
		{0, 2, 2, 2, 0, 1, 1, 1, 0, 1, 1, 1, 0, 2, 2, 2},
		{0, 0, 0, 2, 1, 1, 1, 2, 1, 1, 1, 2, 0, 0, 0, 2},
		{0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 2, 2, 2, 2},
		{0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 1, 2, 2, 1, 1, 2},
		{0, 1, 1, 0, 0, 1, 1, 0, 2, 2, 2, 2, 2, 2, 2, 2},
		{0, 0, 2, 2, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 2, 2},
		{0, 0, 2, 2, 1, 1, 2, 2, 1, 1, 2, 2, 0, 0, 2, 2},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 1, 2},
		{0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1},
		{0, 2, 2, 2, 1, 2, 2, 2, 0, 2, 2, 2, 1, 2, 2, 2},
		{0, 1, 0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
		{0, 1, 1, 1, 2, 0, 1, 1, 2, 2, 0, 1, 2, 2, 2, 0},
	},
}

var bc7AnchorIndexTable = [4][64]uint8{
	{
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
	},
	{
		15, 15, 15, 15, 15, 15, 15, 15,
		15, 15, 15, 15, 15, 15, 15, 15,
		15, 2, 8, 2, 2, 8, 8, 15,
		2, 8, 2, 2, 8, 8, 2, 2,
		15, 15, 6, 8, 2, 8, 15, 15,
		2, 8, 2, 2, 2, 15, 15, 6,
		6, 2, 6, 8, 15, 15, 2, 2,
		15, 15, 15, 15, 15, 2, 2, 15,
	},
	{
		3, 3, 15, 15, 8, 3, 15, 15,
		8, 8, 6, 6, 6, 5, 3, 3,
		3, 3, 8, 15, 3, 3, 6, 10,
		5, 8, 8, 6, 8, 5, 15, 15,
		8, 15, 3, 5, 6, 10, 8, 15,
		15, 3, 15, 5, 15, 15, 15, 15,
		3, 15, 5, 5, 5, 8, 5, 10,
		5, 10, 8, 13, 15, 12, 3, 3,
	},
	{
		15, 8, 8, 3, 15, 15, 3, 8,
		15, 15, 15, 15, 15, 15, 15, 8,
		15, 8, 15, 3, 15, 8, 15, 8,
		3, 15, 6, 10, 15, 15, 10, 8,
		15, 3, 15, 10, 10, 8, 9, 10,
		6, 15, 8, 15, 3, 6, 6, 8,
		15, 3, 15, 15, 15, 15, 15, 15,
		15, 15, 15, 15, 3, 15, 15, 8,
	},
}

var bc7Weight2 = []uint8{0, 21, 43, 64}
var bc7Weight3 = []uint8{0, 9, 18, 27, 37, 46, 55, 64}
var bc7Weight4 = []uint8{0, 4, 9, 13, 17, 21, 26, 30, 34,
	38, 43, 47, 51, 55, 60, 64}

func readBC7Endpoints(block []byte, mode uint64, startBit *uint64) (r [6]uint8, g [6]uint8, b [6]uint8, a [6]uint8) {
	numSubsets := bc7ModeInfo[mode].NumSubsets
	colorBits := bc7ModeInfo[mode].ColorPrecision

	for i := 0; i < int(numSubsets)*2; i++ {
		r[i] = getBits(block, startBit, colorBits)
	}
	for i := 0; i < int(numSubsets)*2; i++ {
		g[i] = getBits(block, startBit, colorBits)
	}
	for i := 0; i < int(numSubsets)*2; i++ {
		b[i] = getBits(block, startBit, colorBits)
	}

	alphaBits := bc7ModeInfo[mode].AlphaPrecision
	hasAlpha := alphaBits > 0
	if hasAlpha {
		for i := 0; i < int(numSubsets)*2; i++ {
			a[i] = getBits(block, startBit, alphaBits)
		}
	} else {
		for i := 0; i < int(numSubsets)*2; i++ {
			a[i] = 255
		}
	}

	numPBits := bc7ModeInfo[mode].NumPBits

	if numPBits > 0 {
		for i := 0; i < int(numSubsets)*2; i++ {
			r[i] <<= 1
			g[i] <<= 1
			b[i] <<= 1
			a[i] <<= 1
		}

		if numPBits == numSubsets {
			// One p-bit shared by both endpoints of each subset (mode 1);
			// it applies to the color channels only.
			for s := 0; s < int(numSubsets); s++ {
				if getBit(block, startBit) {
					for i := 2 * s; i < 2*s+2; i++ {
						r[i] |= 1
						g[i] |= 1
						b[i] |= 1
					}
				}
			}
		} else {
			for i := 0; i < int(numSubsets)*2; i++ {
				pBit := getBit(block, startBit)
				if pBit {
					r[i] |= 1
					g[i] |= 1
					b[i] |= 1
					a[i] |= 1
				}
			}
		}

		colorBits++
		alphaBits++
	}

	for i := 0; i < int(numSubsets)*2; i++ {
		r[i] <<= (8 - colorBits)
		g[i] <<= (8 - colorBits)
		b[i] <<= (8 - colorBits)
		a[i] <<= (8 - alphaBits)

		r[i] |= r[i] >> colorBits
		g[i] |= g[i] >> colorBits
		b[i] |= b[i] >> colorBits
		a[i] |= a[i] >> alphaBits
	}

	if !hasAlpha {
		for i := 0; i < int(numSubsets)*2; i++ {
			a[i] = 255
		}
	}

	return
}

func getBC7SubsetIndex(numSubsets, partitionID uint8, pixelIndex int) uint8 {
	if numSubsets == 2 {
		return bc7PartitionTable[0][partitionID][pixelIndex]
	}
	if numSubsets == 3 {
		return bc7PartitionTable[1][partitionID][pixelIndex]
	}
	return 0
}

func isBC7PixelAnchorIndex(subsetIndex, numSubsets uint8, pixelIndex int, partitionID uint8) bool {
	tableIndex := 0
	if subsetIndex == 0 {
		tableIndex = 0
	} else if subsetIndex == 1 && numSubsets == 2 {
		tableIndex = 1
	} else if subsetIndex == 1 && numSubsets == 3 {
		tableIndex = 2
	} else {
		tableIndex = 3
	}

	return int(bc7AnchorIndexTable[tableIndex][partitionID]) == pixelIndex
}

func DecompressBC7(buf []uint8, r io.Reader, width, height int, info Info) error {
	if info.ColorModel != color.NRGBAModel {
		return errors.New("BC7 compression expects NRGBA color model")
	}
	return decodeBlocks(buf, r, width, height, 16, 4, decodeBC7Block)
}

func decodeBC7Block(block []byte, texels *[16][4]uint8) error {
	startBit := uint64(0)
	for startBit <= 8 && !getBit(block, &startBit) {
	}
	mode := startBit - 1

	if mode > 7 {
		return fmt.Errorf("invalid mode: %v", mode)
	}

	modeInfo := &bc7ModeInfo[mode]
	numSubsets := modeInfo.NumSubsets
	partitionID := uint8(0)

	if modeInfo.PartitionBits > 0 {
		partitionID = getBits(block, &startBit, modeInfo.PartitionBits)
		if partitionID > 63 {
			return fmt.Errorf("invalid partition ID: %v", partitionID)
		}
	}

	rotation := uint8(0)
	if modeInfo.RotationBits > 0 {
		rotation = getBits(block, &startBit, modeInfo.RotationBits)
	}

	selectorBit := false
	if modeInfo.IndexSelectionBits > 0 {
		selectorBit = getBit(block, &startBit)
	}

	cR, cG, cB, cA := readBC7Endpoints(block, mode, &startBit)

	indexPrec := modeInfo.IndexPrecision
	index2Prec := modeInfo.Index2Precision

	var alphaIndices [16]uint8
	if selectorBit {
		// The index selection bit swaps the roles of the two index sets:
		// alpha uses the narrow 2-bit indices (read here, before the color
		// indices) and color uses the wide 3-bit ones.
		indexPrec = 3
		if getBit(block, &startBit) {
			alphaIndices[0] = 1
		}
		for i := 1; i < 16; i++ {
			alphaIndices[i] = getBits(block, &startBit, 2)
		}
	}

	var numBits uint8
	var subsetIndices [16]uint8
	var colorIndices [16]uint8
	for i := 0; i < 16; i++ {
		subsetIndices[i] = getBC7SubsetIndex(numSubsets, partitionID, i)
		numBits = indexPrec
		if isBC7PixelAnchorIndex(subsetIndices[i], numSubsets, i, partitionID) {
			numBits--
		}
		colorIndices[i] = getBits(block, &startBit, numBits)
	}

	if index2Prec > 0 && !selectorBit {
		alphaIndices[0] = getBits(block, &startBit, index2Prec-1)
		for i := 1; i < 16; i++ {
			alphaIndices[i] = getBits(block, &startBit, index2Prec)
		}
	}

	for t := 0; t < 16; t++ {
		c0 := 2 * subsetIndices[t]
		c1 := 2*subsetIndices[t] + 1
		c2 := colorIndices[t]

		weight := uint8(64)
		switch indexPrec {
		case 2:
			if int(c2) < len(bc7Weight2) {
				weight = bc7Weight2[c2]
			}
		case 3:
			if int(c2) < len(bc7Weight3) {
				weight = bc7Weight3[c2]
			}
		default:
			if int(c2) < len(bc7Weight4) {
				weight = bc7Weight4[c2]
			}
		}

		r := uint8(((64-int(weight))*int(cR[c0]) + int(weight)*int(cR[c1]) + 32) >> 6)
		g := uint8(((64-int(weight))*int(cG[c0]) + int(weight)*int(cG[c1]) + 32) >> 6)
		b := uint8(((64-int(weight))*int(cB[c0]) + int(weight)*int(cB[c1]) + 32) >> 6)
		a := uint8(((64-int(weight))*int(cA[c0]) + int(weight)*int(cA[c1]) + 32) >> 6)

		if index2Prec > 0 {
			a0 := alphaIndices[t]
			if int(a0) < len(bc7Weight2) {
				weight = bc7Weight2[a0]
			}
			if index2Prec == 3 && !selectorBit && int(a0) < len(bc7Weight3) {
				weight = bc7Weight3[a0]
			}
			a = uint8(((64-int(weight))*int(cA[c0]) + int(weight)*int(cA[c1]) + 32) >> 6)
		}

		switch rotation {
		case 1:
			a, r = r, a
		case 2:
			a, g = g, a
		case 3:
			a, b = b, a
		}

		texels[t] = [4]uint8{r, g, b, a}
	}
	return nil
}
