package bundle

import (
	"encoding/binary"
	"strings"
)

// MurmurHash64A implements the 64-bit MurmurHash2 algorithm as used by modern PoE (≥3.21.2)
// Reference: https://github.com/poe-tool-dev/poe-dat-viewer/blob/main/lib/src/utils/murmur2.ts
func MurmurHash64A(data []byte, seed uint64) uint64 {
	const (
		m = 0xc6a4a7935bd1e995
		r = 47
	)

	// Initialize hash with seed XOR length
	h := seed ^ (uint64(len(data)) * m)

	// Process 8-byte chunks
	remainder := len(data) & 7
	alignedLength := len(data) - remainder

	// Process aligned 8-byte chunks
	for i := 0; i < alignedLength; i += 8 {
		// Extract 8 bytes as little-endian uint64
		k := binary.LittleEndian.Uint64(data[i : i+8])

		k *= m
		k ^= k >> r
		k *= m

		h ^= k
		h *= m
	}

	// Handle remaining bytes (less than 8)
	switch remainder {
	case 7:
		h ^= uint64(data[alignedLength+6]) << 48
		fallthrough
	case 6:
		h ^= uint64(data[alignedLength+5]) << 40
		fallthrough
	case 5:
		h ^= uint64(data[alignedLength+4]) << 32
		fallthrough
	case 4:
		h ^= uint64(data[alignedLength+3]) << 24
		fallthrough
	case 3:
		h ^= uint64(data[alignedLength+2]) << 16
		fallthrough
	case 2:
		h ^= uint64(data[alignedLength+1]) << 8
		fallthrough
	case 1:
		h ^= uint64(data[alignedLength+0])
		h *= m
	}

	// Final avalanche
	h ^= h >> r
	h *= m
	h ^= h >> r

	return h
}

// MurmurHashPath computes MurmurHash64A of a lowercase path string with seed 0x1337b33f
func MurmurHashPath(path string) uint64 {
	const seed = 0x1337b33f
	lowerPath := strings.ToLower(path)
	return MurmurHash64A([]byte(lowerPath), seed)
}

// FNVHashPath computes FNV-1a hash with "++" suffix for legacy PoE (≤3.21.2)
func FNVHashPath(path string) uint64 {
	const (
		fnvBasis = uint64(0xcbf29ce484222325)
		fnvPrime = uint64(0x100000001b3)
	)

	// Convert path to lowercase and add "++" suffix
	data := []byte(strings.ToLower(path) + "++")

	// FNV-1a algorithm
	hash := fnvBasis
	for _, b := range data {
		hash ^= uint64(b)
		hash *= fnvPrime
	}

	return hash
}

