#include "harness.h"
#include "redis/src/server.h"

void initServer(void);
void redisOutOfMemoryHandler(size_t);
void initServerConfig(void);
void InitServerLast(void);
void initListeners(void);
aofManifest *aofManifestCreate(void);

typedef struct redisObject robj;
extern struct redisCommand redisCommandTable[];
// small optimization we can make with modules to avoid
// dict lookup every command execution
#define MAX_CMDOP 0x1000
struct redisCommand *fuzzCommandTable[MAX_CMDOP];
int n_defer_register;
struct {
	sds s;
	int op;
} defer_register[MAX_CMDOP];

client *gcl;

// override redis functions that interfere with crash handling
/*__attribute__((used)) void setupSignalHandlers(void) {}
__attribute__((used)) void setupDebugSigHandlers(void) {}
__attribute__((used)) void _serverAssert(const char *estr, const char *file, int line) {
	fprintf(stderr, "%s:%d %s\n", file, line, estr);
	assert(0);
}*/

void __wrap_setupSignalHandlers(void) {}
void __wrap_setupDebugSigHandlers(void) {}
void __wrap__serverAssert(const char *estr, const char *file, int line) {
    fprintf(stderr, "%s:%d %s\n", file, line, estr);
    assert(0);
}

void oom_handle(size_t allocation_size) {}

void server_init(void)
{
	struct timeval tv;
	tzset();
	zmalloc_set_oom_handler(oom_handle);
	gettimeofday(&tv,NULL);
	srand(time(NULL)^getpid()^tv.tv_usec);
	srandom(time(NULL)^getpid()^tv.tv_usec);
	init_genrand64(((long long) tv.tv_sec * 1000000 + tv.tv_usec) ^ getpid());
	crc64_init();
	umask(server.umask = umask(0777));
	uint8_t hashseed[16];
	getRandomBytes(hashseed,sizeof(hashseed));
	dictSetHashFunctionSeed(hashseed);
	initServerConfig();
	ACLInit();
	moduleInitModulesSystem();
	connTypeInitialize();
	initServer();
	InitServerLast();
	server.aof_manifest = aofManifestCreate();
	server.lazyfree_lazy_user_del = 0;
	server.lazyfree_lazy_user_flush = 0;
	server.lazyfree_lazy_server_del = 0;
	server.lazyfree_lazy_eviction = 0;
	server.lazyfree_lazy_expire = 0;
	server.active_defrag_enabled = 0;
}

void server_listen(void)
{
	initListeners();
	aeMain(server.el);
	aeDeleteEventLoop(server.el);
}

void client_reset(client *c)
{
	for (int i = 0; i < c->argc; i++)
		decrRefCount(c->argv[i]);
	for (int i = 0; i < c->original_argc; i++)
		decrRefCount(c->original_argv[i]);
	c->argc = 0;
	c->original_argc = 0;
	c->argv_len = 0;
	if (c->original_argv) {
		zfree(c->original_argv);
		c->original_argv = 0;
	}
	if (c->argv) {
		zfree(c->argv);
		c->argv = 0;
	}
}

void harness_set_argc(size_t argc)
{
	gcl->argc = gcl->argv_len = argc;
	gcl->argv = zmalloc(sizeof(robj*)*argc);
}

void harness_add_argv(int idx, const char *s, size_t len)
{
	gcl->argv[idx] = createStringObject(s, len);
}

lua_State *mylua;

void redisProtocolToLuaType(lua_State *lua, char* reply);
void lua_parse_reply()
{
	char *reply;
    if (listLength(gcl->reply) == 0 && (size_t)gcl->bufpos < gcl->buf_usable_size) {
        /* This is a fast path for the common case of a reply inside the
         * client static buffer. Don't create an SDS string but just use
         * the client buffer directly. */
        gcl->buf[gcl->bufpos] = '\0';
        reply = gcl->buf;
        gcl->bufpos = 0;
    } else {
        reply = sdsnewlen(gcl->buf,gcl->bufpos);
        gcl->bufpos = 0;
        while(listLength(gcl->reply)) {
            clientReplyBlock *o = listNodeValue(listFirst(gcl->reply));

            reply = sdscatlen(reply,o->buf,o->used);
            listDelNode(gcl->reply,listFirst(gcl->reply));
        }
    }
	//printf("reply: %s\n", reply);
    redisProtocolToLuaType(mylua,reply);
    if (reply != gcl->buf) sdsfree(reply);
    gcl->reply_bytes = 0;
	//lua_pop(mylua, 1);
}

void lua_state_init(void)
{
	mylua = createLuaState();
}

void lua_state_destroy(void)
{
	lua_gc(mylua, LUA_GCCOLLECT, 0);
	lua_close(mylua);
}

void harness_call_cmd(int op)
{
	//gcl->cmd = gcl->realcmd = &redisCommandTable[op];
	gcl->cmd = gcl->realcmd = fuzzCommandTable[op];
	call(gcl, CMD_CALL_NONE);
	lua_parse_reply();
	client_reset(gcl);
}

void harness_init(void)
{
	server_init();
	resetServerSaveParams();
	// RediSearch-focused fuzzer: only load the search module.
	moduleLoad("./src/redisearch.so", 0, 0, 0);
	//moduleLoad("./src/redisbloom.so", 0, 0, 0);
	//moduleLoad("./src/redistimeseries.so", 0, 0, 0);
	gcl = createClient(0);
	gcl->flags |= CLIENT_SCRIPT;
	for (int i = 0; i < n_defer_register; i++) {
		struct redisCommand *cmd = dictFetchValue(server.commands, defer_register[i].s);
		//printf("command: %s\n", defer_register[i].s);
		assert(cmd);
		fuzzCommandTable[defer_register[i].op] = cmd;
		sdsfree(defer_register[i].s);
	}
	/*gcl->conn = zcalloc(sizeof(connection));
    gcl->conn->type = connectionTypeTcp();
    gcl->conn->fd = -1;
    gcl->conn->iovcnt = IOV_MAX;*/
}

const char *query_command_table(int idx)
{
	return redisCommandTable[idx].declared_name;
}

void core_command_register(int op)
{
	fuzzCommandTable[op] = &redisCommandTable[op];
}

void module_command_register(int op, const char *name)
{
	defer_register[n_defer_register].s = sdsnew(name);
	sdstolower(defer_register[n_defer_register].s);
	defer_register[n_defer_register++].op = op;
}

void harness_call_cmd_by_name(const char *name, size_t len) {
    sds key = sdsnewlen(name, len);
    sdstolower(key);
    struct redisCommand *cmd = dictFetchValue(server.commands, key);
    sdsfree(key);
    if (!cmd) {
        fprintf(stderr, "unknown command: %.*s\n", (int)len, name);
        return;
    }
    gcl->cmd = gcl->realcmd = cmd;
    call(gcl, CMD_CALL_NONE);
	lua_parse_reply();
	client_reset(gcl);
}

// Invoke `argc` strings as a single Redis command on the global client.
// Used to seed deterministic state (index + sample docs) before each
// fuzz iteration so FT.* commands exercise real index code paths rather
// than early-exiting on "no such index".
static void harness_invoke(int argc, const char *const *argv)
{
    harness_set_argc((size_t)argc);
    for (int i = 0; i < argc; i++) {
        harness_add_argv(i, argv[i], strlen(argv[i]));
    }
    harness_call_cmd_by_name(argv[0], strlen(argv[0]));
}

// Recreate the canonical search index + a few docs. Called after FLUSHALL
// at the start of every fuzz iteration so the fuzzer always has a known
// index ("idx") and prefixed keys ("doc:*") to operate against.
void harness_seed_search(void)
{
    {
        // Note: VECTOR field intentionally omitted from the seed. Loading a
        // binary-safe FLOAT32 blob via HSET would require a length-aware
        // helper (strlen() on \x00-prefixed floats truncates). The fuzzer
        // can still create vector indexes itself via FT.CREATE.
        const char *argv[] = {
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT", "SORTABLE",
            "body",  "TEXT",
            "n",     "NUMERIC", "SORTABLE",
            "t",     "TAG", "SORTABLE",
            "loc",   "GEO"
        };
        harness_invoke((int)(sizeof(argv)/sizeof(argv[0])), argv);
    }
    {
        const char *argv[] = {
            "HSET", "doc:1",
            "title", "hello world",
            "body",  "redis search demo document",
            "n",     "42",
            "t",     "tag1,blue",
            "loc",   "-122.4194,37.7749"
        };
        harness_invoke((int)(sizeof(argv)/sizeof(argv[0])), argv);
    }
    {
        const char *argv[] = {
            "HSET", "doc:2",
            "title", "foo bar baz",
            "body",  "another sample document for the fuzzer",
            "n",     "7",
            "t",     "tag2,red",
            "loc",   "-0.1276,51.5074"
        };
        harness_invoke((int)(sizeof(argv)/sizeof(argv[0])), argv);
    }
    {
        const char *argv[] = {"FT.SUGADD", "sug", "hello", "1"};
        harness_invoke(4, argv);
    }
    {
        const char *argv[] = {"FT.DICTADD", "dict", "hello", "world", "redis"};
        harness_invoke(5, argv);
    }
    {
        const char *argv[] = {"FT.SYNUPDATE", "idx", "g1", "hello", "hi"};
        harness_invoke(5, argv);
    }
}
