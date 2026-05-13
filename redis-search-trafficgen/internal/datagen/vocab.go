package datagen

// Hand-rolled lexicons. Fixed in size to keep seed-driven indices stable
// across runs. Avoid the gofakeit dep — these are sufficient for synthetic
// e-commerce text and match the corpus sizes spec'd in scenarios.

var Brands = []string{
	"Acme", "Globex", "Initech", "Umbrella", "Stark", "Wayne", "Cyberdyne", "Tyrell",
	"Wonka", "Soylent", "Pied", "Hooli", "Vandelay", "Massive", "Sirius", "Oscorp",
	"Yoyodyne", "Aperture", "BlackMesa", "Combine", "Octan", "LexCorp", "Nakatomi", "Weyland",
	"Rekall", "Spacely", "Cogswell", "Planet", "Krusty", "Stay", "Duff", "Buynlarge",
	"Pizzeria", "Genco", "Wernham", "Pendant", "Dunder", "Springshield", "Bluth", "Pawnee",
	"Pearson", "Specter", "Litt", "Goliath", "Hyperion", "Encom", "Tessier", "Yutani",
	"Multivac", "Skynet",
}

var Categories = []string{
	"road", "trail", "track", "gravel", "city", "kids", "electric",
	"helmet", "lights", "lock", "pump", "tools", "tubes", "tires",
	"shoes", "shorts", "jersey", "gloves", "socks", "glasses",
	"bottle", "cage", "saddle", "bar", "stem", "post", "grip",
	"derailleur", "shifter", "brake", "rotor", "cassette", "chain", "crank",
	"pedal", "wheel", "rim", "hub", "spoke", "fork", "shock",
	"frame", "computer", "rack", "fender", "bag", "trainer", "stand",
	"nutrition", "lubricant",
}

var TitleWords = []string{
	"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
	"india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa",
	"quebec", "romeo", "sierra", "tango", "uniform", "victor", "whiskey", "xray",
	"yankee", "zulu", "northern", "southern", "eastern", "western", "central",
	"comfort", "performance", "race", "endurance", "explorer", "trekking",
	"summit", "ridge", "valley", "river", "mountain", "forest", "meadow",
	"horizon", "sunrise", "sunset", "twilight", "midnight", "starlight",
	"phoenix", "falcon", "raven", "hawk", "eagle", "sparrow", "robin",
	"crystal", "sapphire", "emerald", "ruby", "amber", "onyx", "pearl",
	"swift", "rapid", "quick", "brisk", "agile", "nimble", "deft",
}

// CommonTerms (Zipf-ish weighting handled at draw time). Large list keeps
// terms_common corpus interesting.
func CommonTerms() []string {
	out := make([]string, 0, len(TitleWords)+len(Categories))
	out = append(out, TitleWords...)
	out = append(out, Categories...)
	return out
}

// Misspellings drives the PHONETIC matching paths in description text.
var Misspellings = []string{
	"comfertable", "performence", "endurence", "ergonomik", "alluminum",
	"durabel", "lightwait", "waterprof", "weatherprof", "premiumly",
}

// MetroCenters: 20 fixed (lon, lat) pairs anchoring geo data so radius
// queries always hit non-empty results.
var MetroCenters = [][2]float64{
	{-74.006, 40.7128},  // NYC
	{-87.6298, 41.8781}, // Chicago
	{-118.2437, 34.0522},// LA
	{-122.4194, 37.7749},// SF
	{-95.3698, 29.7604}, // Houston
	{-80.1918, 25.7617}, // Miami
	{-71.0589, 42.3601}, // Boston
	{-77.0369, 38.9072}, // DC
	{-122.3321, 47.6062},// Seattle
	{-105.0178, 39.7392},// Denver
	{2.3522, 48.8566},   // Paris
	{-0.1276, 51.5074},  // London
	{13.4050, 52.5200},  // Berlin
	{12.4964, 41.9028},  // Rome
	{34.7818, 32.0853},  // Tel Aviv
	{139.6917, 35.6895}, // Tokyo
	{151.2093, -33.8688},// Sydney
	{116.4074, 39.9042}, // Beijing
	{77.1025, 28.7041},  // Delhi
	{18.4241, -33.9249}, // Cape Town
}
