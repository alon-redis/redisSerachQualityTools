package datagen

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
)

// GenCentroids draws `n` unit vectors of dimension `dim` from a Gaussian
// prior. Deterministic given the rng.
func GenCentroids(rng *rand.Rand, dim, n int) [][]float32 {
	cs := make([][]float32, n)
	for i := range cs {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		L2Normalize(v)
		cs[i] = v
	}
	return cs
}

// MakeVec offsets a centroid by Gaussian noise of width sigma and optionally
// re-normalizes. Caller picks normalize=true for COSINE/IP, false for L2.
func MakeVec(rng *rand.Rand, centroid []float32, sigma float32, normalize bool) []float32 {
	v := make([]float32, len(centroid))
	for i := range v {
		v[i] = centroid[i] + sigma*float32(rng.NormFloat64())
	}
	if normalize {
		L2Normalize(v)
	}
	return v
}

func L2Normalize(v []float32) {
	var s float64
	for _, x := range v {
		s += float64(x) * float64(x)
	}
	if s == 0 {
		// Defensive: avoid NaN; collapse to a fixed unit vector.
		if len(v) > 0 {
			v[0] = 1
		}
		return
	}
	inv := float32(1.0 / math.Sqrt(s))
	for i := range v {
		v[i] *= inv
	}
}

// F32ToBytesLE packs []float32 as little-endian FP32 bytes, matching what
// PARAMS expects for vector query arguments.
func F32ToBytesLE(v []float32) []byte {
	b := make([]byte, 4*len(v))
	for i, x := range v {
		binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(x))
	}
	return b
}
