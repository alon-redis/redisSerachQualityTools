# redis-fuzz
A [LibAFL](https://github.com/AFLplusplus/LibAFL) in-process fuzzer targeting command functions in `redis-server` + selected modules with semantic-awareness.

This branch is **focused on the RediSearch module**: only `redisearch.so` is loaded into the in-process server, every fuzz iteration starts with a fresh canonical index (`idx` on HASH `PREFIX 1 doc:` with TEXT/NUMERIC/TAG/GEO fields plus a seeded suggestion list, dictionary, and synonym group), and the command generator biases search-command argument names (`index`, `dict`, `alias`, `field`, `query`, `prefix`, `synonym_group_id`, …) toward the seeded resources so FT.* calls exercise real index code paths instead of early-exiting on "unknown index". The fuzzer can still create vector indexes itself via generated `FT.CREATE … VECTOR …` calls.

## Building
First, fully initialize all git submodules.
On a Linux system with all dependencies, run `make`.
TODO: provide a Dockerfile to build an image with all dependencies.

## Issues
Bug in RediSearch necessitates this change to build:
`sed -i 's/::template write_element_idx/::write_element_idx/g' ./src/redisearch/bin/linux-x64-release/search-community/_deps/boost-src/boost/qvm/quat_traits.hpp`
At the end of `src/redis/src/server.h`, add
```
#undef serverLog
#define serverLog(level, ...) 1
```
to greatly speed up fuzzing, TODO: do this automatically.

## Running
`./run N` spawns N fuzzing processes, the first process acts as a manager that subsequent processes communicate with.

## Config
Config is specified by the path in the `FUZZ_CONFIG` environment variable and defaults to `defconfig.json`. The shipped `defconfig.json`:

- enables only `core` and `search` modules,
- heavily weights the `search` group (×8) and individual hot FT.* commands (`FT.SEARCH`, `FT.AGGREGATE`, `FT.PROFILE`, …),
- blacklists internal/idempotent FT helpers (`FT._LIST`, `FT.CONFIG`, `FT._CREATEIFNX`, …) that mostly produce noise.

Adding commands from the global blacklist back into the whitelist will likely trigger false positives. In general, commands with the flag `CMD_NOSCRIPT` that cannot be called from a Lua script should not be whitelisted.

## RediSearch-specific behaviour
The in-process harness ([src/harness.c](src/harness.c) `harness_seed_search`) re-runs the following at the start of every fuzz iteration (after `FLUSHALL`):

```
FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA
  title TEXT SORTABLE
  body  TEXT
  n     NUMERIC SORTABLE
  t     TAG SORTABLE
  loc   GEO
HSET doc:1 title "hello world"  body "redis search demo document"  n 42 t "tag1,blue" loc -122.4194,37.7749
HSET doc:2 title "foo bar baz"  body "another sample document …"  n  7 t "tag2,red"  loc   -0.1276,51.5074
FT.SUGADD sug hello 1
FT.DICTADD dict hello world redis
FT.SYNUPDATE idx g1 hello hi
```

So the fuzzer always has:
- index `idx` (HASH prefix `doc:`, mixed field types incl. VECTOR),
- doc keys `doc:1`, `doc:2`,
- suggestion list `sug`, dictionary `dict`, synonym group `g1`.

The Rust command generator ([src/smith.rs](src/smith.rs) `search_arg_override`, `gen_string_for`, `gen_key_for`) maps search-arg names to these resources ~75–85% of the time, and falls back to random strings otherwise. Tweak the probability constants in those helpers if you want more chaos (lower) or more depth (higher).

## Triage
See [triage.py](triage.py) and [minimize.py](minimize.py) — replay crashing inputs from `./crashes/`, group by top stack frame, and shrink the reproducing command sequence.
