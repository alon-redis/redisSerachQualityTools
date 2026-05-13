package datagen

import (
	"math/rand/v2"
)

// Corpus holds pre-generated query inputs so workers don't construct them
// in the hot path. Built once at the start of `run` from the master seed.
type Corpus struct {
	CommonTerms  []string
	RareTerms    []string
	Brands       []string
	Categories   []string
	Misspellings []string
	GeoPoints    [][2]float64
	QueryVecDesc [][]float32
	QueryVecImg  [][]float32

	// Centroids used by the assertion paths (for clustered query generation
	// and recall ground truth construction).
	DescCentroids [][]float32
	ImgCentroids  [][]float32
	FeatCentroids [][]float32
}

// BuildCorpus deterministically materializes every query input the workers
// will sample from.
func BuildCorpus(
	master uint64,
	descDim, imgDim, featDim, nClusters int,
	commonSize, rareSize, queryVecDescSize, queryVecImgSize, geoSize int,
) *Corpus {
	descRNG := RNG(master, StreamCentroids+0)
	imgRNG := RNG(master, StreamCentroids+1)
	featRNG := RNG(master, StreamCentroids+2)

	c := &Corpus{
		DescCentroids: GenCentroids(descRNG, descDim, nClusters),
		ImgCentroids:  GenCentroids(imgRNG, imgDim, nClusters),
		FeatCentroids: GenCentroids(featRNG, featDim, nClusters),
		Brands:        append([]string{}, Brands...),
		Categories:    append([]string{}, Categories...),
		Misspellings:  append([]string{}, Misspellings...),
	}

	qRNG := RNG(master, StreamQueries)

	// Common terms: drawn from TitleWords + Categories. Repeats allowed so
	// Zipf draws sample over the full distribution.
	common := CommonTerms()
	c.CommonTerms = make([]string, commonSize)
	for i := range c.CommonTerms {
		c.CommonTerms[i] = common[qRNG.IntN(len(common))]
	}

	// Rare terms: synthesize unique-ish suffixed tokens for prefix-expansion stress.
	c.RareTerms = make([]string, rareSize)
	for i := range c.RareTerms {
		base := TitleWords[qRNG.IntN(len(TitleWords))]
		c.RareTerms[i] = base + suffix(qRNG, 4)
	}

	// Query vectors: 90% centroid-targeted, 10% out-of-distribution.
	c.QueryVecDesc = make([][]float32, queryVecDescSize)
	for i := range c.QueryVecDesc {
		c.QueryVecDesc[i] = makeQueryVec(qRNG, c.DescCentroids, descDim, true)
	}
	c.QueryVecImg = make([][]float32, queryVecImgSize)
	for i := range c.QueryVecImg {
		c.QueryVecImg[i] = makeQueryVec(qRNG, c.ImgCentroids, imgDim, true)
	}

	// Geo: sample from metro centers + jitter.
	c.GeoPoints = make([][2]float64, geoSize)
	for i := range c.GeoPoints {
		center := MetroCenters[qRNG.IntN(len(MetroCenters))]
		c.GeoPoints[i] = [2]float64{
			center[0] + 0.02*qRNG.NormFloat64(),
			center[1] + 0.02*qRNG.NormFloat64(),
		}
	}
	return c
}

func makeQueryVec(rng *rand.Rand, centroids [][]float32, dim int, normalize bool) []float32 {
	if rng.Float64() < 0.9 {
		c := centroids[rng.IntN(len(centroids))]
		return MakeVec(rng, c, 0.05, normalize)
	}
	// OOD: random unit vector around the origin.
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	if normalize {
		L2Normalize(v)
	}
	return v
}

func suffix(rng *rand.Rand, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rng.IntN(len(alphabet))]
	}
	return string(b)
}
