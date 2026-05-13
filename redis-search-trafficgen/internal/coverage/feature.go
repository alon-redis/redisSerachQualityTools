// Package coverage tracks which P0 Redis Search features the run exercised.
package coverage

type Feature string

const (
	FeatHashDoc          Feature = "hash_doc"
	FeatJSONDoc          Feature = "json_doc"
	FeatTagSeparator     Feature = "tag_separator"
	FeatTagCaseSensitive Feature = "tag_casesensitive"
	FeatTagSortable      Feature = "tag_sortable"
	FeatTagSuffixtrie    Feature = "tag_with_suffixtrie"
	FeatTextWeight       Feature = "text_weight"
	FeatTextSortable     Feature = "text_sortable"
	FeatTextNoIndex      Feature = "text_noindex"
	FeatTextSuffixtrie   Feature = "text_with_suffixtrie"
	FeatTextPhonetic     Feature = "text_phonetic"
	FeatTextNoStem       Feature = "text_nostem"
	FeatVecFlat          Feature = "vec_flat"
	FeatVecHNSW          Feature = "vec_hnsw"
	FeatVecSVSVamana     Feature = "vec_svs_vamana"
	FeatVecFP32          Feature = "vec_fp32"
	FeatVecFP16          Feature = "vec_fp16"
	FeatDistL2           Feature = "dist_l2"
	FeatDistIP           Feature = "dist_ip"
	FeatDistCosine       Feature = "dist_cosine"
	FeatSearchExact      Feature = "search_exact"
	FeatSearchPrefix     Feature = "search_prefix"
	FeatSearchWildcard   Feature = "search_wildcard"
	FeatSearchFielded    Feature = "search_fielded"
	FeatSearchInFields   Feature = "search_infields"
	FeatBoolAnd          Feature = "bool_and"
	FeatBoolOr           Feature = "bool_or"
	FeatBoolNot          Feature = "bool_not"
	FeatBoolOptional     Feature = "bool_optional"
	FeatBM25             Feature = "bm25"
	FeatHybridRRF        Feature = "hybrid_rrf"
	FeatHybridLinear     Feature = "hybrid_linear"
	FeatKNN              Feature = "knn"
	FeatKNNPrefilter     Feature = "knn_prefilter"
	FeatSortBy           Feature = "sortby"
	FeatLimit            Feature = "limit"
	FeatReturn           Feature = "return"
	FeatLoad             Feature = "load"
	FeatApply            Feature = "apply"
	FeatFilter           Feature = "filter"
	FeatGroupBy          Feature = "groupby"
	FeatReduceCount      Feature = "reduce_count"
	FeatReduceSum        Feature = "reduce_sum"
	FeatReduceAvg        Feature = "reduce_avg"
	FeatReduceMin        Feature = "reduce_min"
	FeatReduceMax        Feature = "reduce_max"
	FeatReduceStddev     Feature = "reduce_stddev"
	FeatReduceQuantile   Feature = "reduce_quantile"
	FeatReduceToList     Feature = "reduce_tolist"
	FeatReduceFirstValue Feature = "reduce_first_value"
	FeatReduceRandSample Feature = "reduce_random_sample"
	FeatTimeout          Feature = "timeout"
	FeatBackgroundIndex  Feature = "background_indexing"
	FeatFTInfo           Feature = "ft_info"
	FeatInfoSearch       Feature = "info_search"
	FeatDialect2         Feature = "dialect_2"
	FeatDialect3         Feature = "dialect_3"
)

// AllFeatures lists every feature the spec enumerates; used by the reporter
// to identify the gaps with zero counts.
func AllFeatures() []Feature {
	return []Feature{
		FeatHashDoc, FeatJSONDoc,
		FeatTagSeparator, FeatTagCaseSensitive, FeatTagSortable, FeatTagSuffixtrie,
		FeatTextWeight, FeatTextSortable, FeatTextNoIndex, FeatTextSuffixtrie, FeatTextPhonetic, FeatTextNoStem,
		FeatVecFlat, FeatVecHNSW, FeatVecSVSVamana, FeatVecFP32, FeatVecFP16,
		FeatDistL2, FeatDistIP, FeatDistCosine,
		FeatSearchExact, FeatSearchPrefix, FeatSearchWildcard, FeatSearchFielded, FeatSearchInFields,
		FeatBoolAnd, FeatBoolOr, FeatBoolNot, FeatBoolOptional,
		FeatBM25, FeatHybridRRF, FeatHybridLinear, FeatKNN, FeatKNNPrefilter,
		FeatSortBy, FeatLimit, FeatReturn, FeatLoad, FeatApply, FeatFilter,
		FeatGroupBy, FeatReduceCount, FeatReduceSum, FeatReduceAvg, FeatReduceMin, FeatReduceMax,
		FeatReduceStddev, FeatReduceQuantile, FeatReduceToList, FeatReduceFirstValue, FeatReduceRandSample,
		FeatTimeout, FeatBackgroundIndex, FeatFTInfo, FeatInfoSearch,
		FeatDialect2, FeatDialect3,
	}
}
