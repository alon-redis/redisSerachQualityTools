package datagen

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
)

// Product is the JSON document written under product:<key>. Field tags match
// the JSONPath aliases declared in internal/schema/product.go.
type Product struct {
	SKU           string    `json:"sku"`
	Brand         string    `json:"brand"`
	Categories    []string  `json:"categories"`
	Title         string    `json:"title"`
	Description   string    `json:"description"`
	InternalNotes string    `json:"internal_notes"`
	Price         float64   `json:"price"`
	Rating        float64   `json:"rating"`
	InStock       string    `json:"in_stock"`
	CreatedTS     int64     `json:"created_ts"`
	StoreLocation string    `json:"store_location"`
	PickupZone    string    `json:"pickup_zone"`
	DescEmbedding []float32 `json:"desc_embedding"`
	ImgEmbedding  []float32 `json:"img_embedding"`
	FeatEmbedding []float32 `json:"feat_embedding"`
}

// AnchorTitle is the search anchor the verification goroutine looks for.
// Kept here so the anchor stays trivially findable across all populators.
const AnchorTitle = "Alon Shmuely QA architect"

// FlatHashFlex projects a Product into the [field, value, ...] sequence
// HSET expects for the Flex (HASH-backed) idx:product. Vectors are encoded
// as little-endian FP32 bytes; FLOAT16 storage isn't supported on Flex.
// Non-indexed fields (price, rating, created_ts, etc.) are still written so
// callers can read them back if needed.
func (p Product) FlatHashFlex() []interface{} {
	cats := ""
	for i, c := range p.Categories {
		if i > 0 {
			cats += "|"
		}
		cats += c
	}
	return []interface{}{
		"sku", p.SKU,
		"brand", p.Brand,
		"categories", cats,
		"title", p.Title,
		"description", p.Description,
		"in_stock", p.InStock,
		"internal_notes", p.InternalNotes,
		"price", p.Price,
		"rating", p.Rating,
		"created_ts", p.CreatedTS,
		"store_location", p.StoreLocation,
		"pickup_zone", p.PickupZone,
		"desc_vec", F32ToBytesLE(p.DescEmbedding),
		"img_vec", F32ToBytesLE(p.ImgEmbedding),
		"feat_vec", F32ToBytesLE(p.FeatEmbedding),
	}
}

// ProductKey returns the stable Redis key for product index `idx`.
func ProductKey(prefix string, master uint64, idx int) string {
	h := sha1.New()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], master)
	h.Write(buf[:])
	h.Write([]byte("product"))
	binary.LittleEndian.PutUint64(buf[:], uint64(idx))
	h.Write(buf[:])
	return prefix + hex.EncodeToString(h.Sum(nil))[:12]
}

// GenProductsStream generates `count` product docs deterministically and
// emits them one at a time on `out`, closing `out` when done (or when
// ctx is cancelled). The product at idx == 0 is the anchor doc and only
// appears when startIdx == 0.
//
// Streaming (vs the older slice-returning variant) keeps peak resident
// memory bounded at the channel buffer size + the consumers' batch
// buffers, regardless of how many docs are generated — necessary for
// multi-million-doc preloads that would otherwise materialize an
// O(count × ~4 KB) slice.
func GenProductsStream(
	ctx context.Context,
	master uint64,
	prefix string,
	startIdx, count int,
	descCentroids, imgCentroids, featCentroids [][]float32,
	out chan<- ProductDoc,
) {
	defer close(out)
	productsRNG := RNG(master, StreamProducts)
	geoRNG := RNG(master, StreamGeo)
	descRNG := RNG(master, StreamVectorsDesc)
	imgRNG := RNG(master, StreamVectorsImg)
	featRNG := RNG(master, StreamVectorsFeat)

	for i := 0; i < count; i++ {
		idx := startIdx + i
		var p Product
		if idx == 0 {
			p = makeAnchorProduct(productsRNG, geoRNG, descRNG, imgRNG, featRNG,
				descCentroids, imgCentroids, featCentroids)
		} else {
			p = makeProduct(idx, productsRNG, geoRNG, descRNG, imgRNG, featRNG,
				descCentroids, imgCentroids, featCentroids)
		}
		doc := ProductDoc{Key: ProductKey(prefix, master, idx), Product: p, Index: idx}
		select {
		case <-ctx.Done():
			return
		case out <- doc:
		}
	}
}

type ProductDoc struct {
	Key     string
	Product Product
	Index   int
}

func makeProduct(
	idx int,
	productsRNG, geoRNG, descRNG, imgRNG, featRNG *rand.Rand,
	descCentroids, imgCentroids, featCentroids [][]float32,
) Product {
	brand := Brands[idx%len(Brands)]

	// 1-3 categories with a Zipf prior so the head of the taxonomy stays popular.
	nCats := 1 + productsRNG.IntN(3)
	cats := make([]string, 0, nCats)
	seen := map[int]bool{}
	for len(cats) < nCats {
		ci := zipf(productsRNG, len(Categories))
		if seen[ci] {
			continue
		}
		seen[ci] = true
		cats = append(cats, Categories[ci])
	}

	titleW1 := TitleWords[productsRNG.IntN(len(TitleWords))]
	titleW2 := TitleWords[productsRNG.IntN(len(TitleWords))]
	title := fmt.Sprintf("%s %s %s", brand, titleW1, titleW2)

	descPieces := []string{
		fmt.Sprintf("%s engineered for %s riders.", brand, cats[0]),
		fmt.Sprintf("Designed with %s and %s in mind.", titleW1, titleW2),
	}
	if productsRNG.Float64() < 0.5 {
		descPieces = append(descPieces, "Notably "+Misspellings[productsRNG.IntN(len(Misspellings))]+".")
	}
	description := strings.Join(descPieces, " ")

	notes := fmt.Sprintf("NOINDEX-%d internal review pending", idx)

	price := math.Exp(5+0.6*productsRNG.NormFloat64()) // ~$50-$2000
	if price < 1 {
		price = 1
	}
	rating := 1 + 4*betaSample(productsRNG, 2, 1)

	inStock := "true"
	if productsRNG.Float64() < 0.15 {
		inStock = "false"
	}

	// Last ~2 years (60 * 86400 * 12 ≈ 730 days)
	createdTS := int64(1700000000) + int64(productsRNG.IntN(2*365*24*3600))

	metroIdx := zipf(geoRNG, len(MetroCenters))
	center := MetroCenters[metroIdx]
	lon := center[0] + 0.03*geoRNG.NormFloat64()
	lat := center[1] + 0.03*geoRNG.NormFloat64()
	storeLoc := fmt.Sprintf("%.6f,%.6f", lon, lat)
	pickup := wktSquare(lon, lat, 0.05)

	descCluster := hashIdx(cats[0], len(descCentroids))
	imgCluster := hashIdx(brand, len(imgCentroids))
	featCluster := hashIdx(cats[0], len(featCentroids))

	return Product{
		SKU:           fmt.Sprintf("SKU-%06d", idx),
		Brand:         brand,
		Categories:    cats,
		Title:         title,
		Description:   description,
		InternalNotes: notes,
		Price:         price,
		Rating:        rating,
		InStock:       inStock,
		CreatedTS:     createdTS,
		StoreLocation: storeLoc,
		PickupZone:    pickup,
		DescEmbedding: MakeVec(descRNG, descCentroids[descCluster], 0.15, true),
		ImgEmbedding:  MakeVec(imgRNG, imgCentroids[imgCluster], 0.10, true),
		FeatEmbedding: MakeVec(featRNG, featCentroids[featCluster], 0.30, false),
	}
}

func makeAnchorProduct(
	_, _, descRNG, imgRNG, featRNG *rand.Rand,
	descCentroids, imgCentroids, featCentroids [][]float32,
) Product {
	return Product{
		SKU:           "ANCHOR-0",
		Brand:         "AnchorBrand",
		Categories:    []string{"anchor"},
		Title:         AnchorTitle,
		Description:   "Anchor doc seeded for trafficgen verification.",
		InternalNotes: "NOINDEX-0 anchor",
		Price:         99.99,
		Rating:        5.0,
		InStock:       "true",
		CreatedTS:     1700000000,
		StoreLocation: "0.000000,0.000000",
		PickupZone:    wktSquare(0, 0, 0.05),
		DescEmbedding: MakeVec(descRNG, descCentroids[0], 0.01, true),
		ImgEmbedding:  MakeVec(imgRNG, imgCentroids[0], 0.01, true),
		FeatEmbedding: MakeVec(featRNG, featCentroids[0], 0.01, false),
	}
}

func zipf(rng *rand.Rand, n int) int {
	if n <= 1 {
		return 0
	}
	// math/rand/v2 doesn't ship a Zipf distribution. Bias toward small
	// indices with x = floor(n * u^2), where u ~ Uniform(0,1). Close enough
	// to Zipfian for taxonomy / country / metro sampling.
	u := rng.Float64()
	i := int(float64(n) * u * u)
	if i >= n {
		i = n - 1
	}
	return i
}

func betaSample(rng *rand.Rand, a, b float64) float64 {
	// Beta from two Gamma samples; Gamma via Marsaglia & Tsang.
	x := gammaSample(rng, a)
	y := gammaSample(rng, b)
	return x / (x + y)
}

func gammaSample(rng *rand.Rand, alpha float64) float64 {
	if alpha < 1 {
		return gammaSample(rng, alpha+1) * math.Pow(rng.Float64(), 1/alpha)
	}
	d := alpha - 1.0/3.0
	c := 1.0 / math.Sqrt(9*d)
	for {
		x := rng.NormFloat64()
		v := 1 + c*x
		if v <= 0 {
			continue
		}
		v = v * v * v
		u := rng.Float64()
		if u < 1-0.0331*(x*x*x*x) {
			return d * v
		}
		if math.Log(u) < 0.5*x*x+d*(1-v+math.Log(v)) {
			return d * v
		}
	}
}

func hashIdx(s string, n int) int {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return int(h % uint32(n))
}

func wktSquare(lon, lat, half float64) string {
	return fmt.Sprintf("POLYGON((%.6f %.6f, %.6f %.6f, %.6f %.6f, %.6f %.6f, %.6f %.6f))",
		lon-half, lat-half,
		lon-half, lat+half,
		lon+half, lat+half,
		lon+half, lat-half,
		lon-half, lat-half,
	)
}
