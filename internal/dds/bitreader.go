package dds

// getBit and getBits saturate on overrun: the cursor always advances by the
// requested amount and out-of-range bits read as zero.
func getBit(block []uint8, startBit *uint64) bool {
	index := (*startBit) >> 3
	base := (*startBit) - (index << 3)
	(*startBit)++
	if index >= uint64(len(block)) {
		return false
	}
	return (block[index]>>base)&1 != 0
}

func getBits(block []uint8, startBit *uint64, numBits uint8) uint8 {
	index := (*startBit) >> 3
	base := (*startBit) - (index << 3)
	*startBit += uint64(numBits)
	if index >= uint64(len(block)) {
		return 0
	}
	res := (block[index] >> base) & ((1 << numBits) - 1)
	if base+uint64(numBits) > 8 && index+1 < uint64(len(block)) {
		firstBits := 8 - base
		nextBits := uint64(numBits) - firstBits
		res = (block[index] >> base) |
			((block[index+1] & ((1 << nextBits) - 1)) << firstBits)
	}
	return res
}
