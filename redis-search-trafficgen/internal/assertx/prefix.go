package assertx

import (
	"fmt"
	"math/rand/v2"
	"strings"
)

// PrefixMembership verifies every returned title begins with the prefix.
// Cheap: works on the existing PrefixOp result with no extra Redis traffic.
func PrefixMembership(rng *rand.Rand, sampleRate float64, severity Severity, prefix string, titles []string) (Result, bool) {
	if sampleRate <= 0 || rng.Float64() >= sampleRate {
		return Result{}, false
	}
	if len(titles) == 0 {
		// No titles to verify; record as passing-but-not-meaningful so the
		// reporter still shows the assertion fired.
		return Result{Name: "prefix_membership", Passed: true, Severity: severity}, true
	}
	lp := strings.ToLower(prefix)
	for _, t := range titles {
		if !anyTokenHasPrefix(t, lp) {
			return Result{
				Name:     "prefix_membership",
				Passed:   false,
				Severity: severity,
				Detail:   fmt.Sprintf("no token in title %q starts with %q", t, prefix),
			}, true
		}
	}
	return Result{Name: "prefix_membership", Passed: true, Severity: severity}, true
}

// anyTokenHasPrefix matches RediSearch's TEXT tokenization: a query like
// `@title:hel*` matches any document whose title contains a word beginning
// with "hel". Splits on whitespace and common punctuation.
func anyTokenHasPrefix(title, lcPrefix string) bool {
	lower := strings.ToLower(title)
	tokens := strings.FieldsFunc(lower, func(r rune) bool {
		switch r {
		case ' ', '\t', '\n', '.', ',', '-', '_', '/', '\\', '(', ')', '[', ']', '{', '}', ':', ';', '!', '?', '"', '\'':
			return true
		}
		return false
	})
	for _, tok := range tokens {
		if strings.HasPrefix(tok, lcPrefix) {
			return true
		}
	}
	return false
}
