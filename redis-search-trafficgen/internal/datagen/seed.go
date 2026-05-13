// Package datagen produces deterministic synthetic products, events,
// vectors, and a query corpus from a single master seed.
package datagen

import "math/rand/v2"

const (
	StreamProducts    uint64 = 1
	StreamEvents      uint64 = 2
	StreamQueries     uint64 = 3
	StreamVectorsDesc uint64 = 4
	StreamVectorsImg  uint64 = 5
	StreamVectorsFeat uint64 = 6
	StreamGeo         uint64 = 7
	StreamCentroids   uint64 = 8
	StreamAnchor      uint64 = 9
)

// RNG builds a PCG-seeded *rand.Rand for a given (master, stream) pair.
func RNG(master, stream uint64) *rand.Rand {
	return rand.New(rand.NewPCG(master, stream))
}

// WorkerRNG derives a per-worker stream from (master, stream, workerID) so
// concurrent draws are independent and replay-stable.
func WorkerRNG(master, stream uint64, workerID int) *rand.Rand {
	return rand.New(rand.NewPCG(master, stream<<32|uint64(workerID)))
}
