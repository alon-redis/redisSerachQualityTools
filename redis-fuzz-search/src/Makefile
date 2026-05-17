CC = $(shell pwd)/../target/debug/libafl_cc
CXX = $(shell pwd)/../target/debug/libafl_cxx
CFLAGS = -fno-lto

PRINTMK := $(CURDIR)/print.mk
define redis_var
$(shell $(MAKE) -s -f $(PRINTMK) -C redis/src print-$(1))
endef

REDIS_SERVER_OBJ := $(call redis_var,REDIS_SERVER_OBJ)
REDIS_VEC_SETS_OBJ := $(call redis_var,REDIS_VEC_SETS_OBJ)

INCLUDES = -Iredis/src -Iredis/deps/linenoise -Iredis/deps/lua/src -Iredis/deps/hdr_histogram -Iredis/deps/fpconv -Iredis/deps/fast_float
REDIS_DEPS_LIBS = redis/deps/hiredis/libhiredis.a redis/deps/lua/src/liblua.a redis/deps/hdr_histogram/libhdrhistogram.a redis/deps/fpconv/libfpconv.a redis/deps/fast_float/libfast_float.a

RUST_SRC = lib.rs smith.rs mutator.rs
# RediSearch-focused build: only redisearch.so is required at link time.
REDIS_MODULES = redisearch.so

WRAPS = -Wl,--wrap=setupSignalHandlers \
		-Wl,--wrap=setupDebugSigHandlers \
		-Wl,--wrap=_serverAssert

all: ../fuzz ../replay

../fuzz: harness.c stub.c libredis.a $(RUST_SRC) $(REDIS_MODULES)
	$(CC) --libafl -o $@ harness.c stub.c $(INCLUDES) libredis.a $(REDIS_DEPS_LIBS) $(WRAPS)

../replay: replay.c harness.c libredis.a
	$(CC) -o $@ harness.c replay.c $(INCLUDES) libredis.a $(REDIS_DEPS_LIBS) -Wno-deprecated-declarations

libredis.a:
	CC="$(CC)" CFLAGS="$(CFLAGS)" \
	   $(MAKE) -C redis BUILD_TLS=no MALLOC=libc
	@objcopy --weaken-symbol=main redis/src/server.o
	#@objcopy --redefine-sym setupSignalHandlers=redis_setupSignalHandlers redis/src/server.o
	#@objcopy --redefine-sym setupDebugSigHandlers=redis_setupDebugSigHandlers redis/src/debug.o
	#@objcopy --redefine-sym _serverAssert=redis_serverAssert redis/src/debug.o
	#@objcopy --redefine-sym _serverAssert=redis_serverAssert redis/src/redisassert.o
	@objcopy --globalize-symbol=redisProtocolToLuaType redis/src/script_lua.o
	@ar rcs -o libredis.a \
		$(addprefix redis/src/, $(REDIS_SERVER_OBJ) $(REDIS_VEC_SETS_OBJ))

redisbloom.so:
	CC="$(CC)" CXX="$(CXX)" CFLAGS="$(CFLAGS)" \
	   $(MAKE) -C RedisBloom TESTS=0
	@mv RedisBloom/bin/linux-x64-release/$@ .

redistimeseries.so:
	CC="$(CC) -fblocks" CXX="$(CXX)" CFLAGS="$(CFLAGS)" \
	   $(MAKE) -C redistimeseries TESTS=0
	@mv redistimeseries/bin/linux-x64-release/$@ .

redisearch.so:
	CC="$(CC)" CXX="$(CXX)" CFLAGS="$(CFLAGS)" \
	   $(MAKE) -C redisearch build TESTS=0 IGNORE_MISSING_DEPS=1
	@mv redisearch/bin/linux-x64-release/search-community/$@ .

rejson.so:
	CC="$(CC)" CXX="$(CXX)" CFLAGS="$(CFLAGS)" \
	   $(MAKE) -C redisjson
	@mv redisjson/bin/linux-x64-release/$@ .

.PHONY: clean
clean:
	rm -f ../fuzz libredis.a $(REDIS_MODULES)
	make -C redis/src distclean
	make -C RedisBloom clean
	make -C redisearch clean ALL=1
	make -C redistimeseries clean
	make -C redisjson clean
