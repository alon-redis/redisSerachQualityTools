#include "harness.h"
#include "redis/src/server.h"
#include <ctype.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_CMD   0x10000
#define MAX_ARGV  0x400

static void replay_command(char *cmd, ssize_t len)
{
    const char *argvv[MAX_ARGV];
    size_t argvl[MAX_ARGV];
    int argc = 0;

    char *p = cmd, *end = cmd + len;
    while (p < end) {
        while (p < end && isspace((unsigned char)*p)) p++;
        if (p >= end) break;
        char *start = p;
        while (p < end && !isspace((unsigned char)*p)) p++;
        if (argc < MAX_ARGV) {
            argvv[argc] = start;
            argvl[argc] = (size_t)(p - start);
            argc++;
        }
    }
    if (argc == 0) return;

    harness_set_argc(argc);
    for (int i = 0; i < argc; i++) {
        harness_add_argv(i, argvv[i], argvl[i]);
    }
    harness_call_cmd_by_name(argvv[0], argvl[0]);
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        puts("usage: replay <command_log>");
        return 1;
    }
    int fd = open(argv[1], O_RDONLY);
    if (fd < 0) { perror("open"); return 1; }

    harness_init();
	lua_state_init();
	// Match the fuzzer's per-iteration seed (idx + docs + sug/dict/syn) so
	// crashing FT.* sequences reproduce against the same starting state.
	harness_seed_search();

    char line[MAX_CMD];
    char chunk[0x8000];
    ssize_t llen = 0;

    for (;;) {
        ssize_t n = read(fd, chunk, sizeof(chunk));
        if (n <= 0) break;
        for (ssize_t i = 0; i < n; i++) {
            char c = chunk[i];
            if (c == '\n') {
                replay_command(line, llen);
                llen = 0;
            } else if (llen < (ssize_t)sizeof(line) - 1) {
                line[llen++] = c;
            }
        }
    }
    if (llen) replay_command(line, llen);
    close(fd);
	lua_state_destroy();
    return 0;
}
/*#include "harness.h"
#include "redis/src/server.h"
#include "hashtable.c"

#define MAX_CMD 0x10000

extern struct redisCommand redisCommandTable[];
Hashtable *op_map;

void replay_command(char *cmd, ssize_t len)
{
	int idx = 0, op = -1;
	char *p = cmd;
	char *end = cmd + len;
	int argc = 0;
	while (p < end) {
		while (p < end && isspace(*p))
			p++;
		if (p >= end)
			break;
		while (p < end && !isspace(*p))
			p++;
		argc++;
	}
	p = cmd;
	while (p < end) {
		while (p < end && isspace(*p))
			p++;
		if (p >= end)
			break;
		char *arg_start = p;
		while (p < end && !isspace(*p))
			p++;
		int arg_len = p - arg_start;
		if (op < 0) {
			if (!ht_get(op_map, arg_start, arg_len, &op)) {
				printf("invalid command: %.*s\n", (int)len, cmd);
				exit(1);
			}
			harness_set_argc(argc);
		}
		harness_add_argv(idx++, arg_start, arg_len);
	}
	if (op >= 0)
		harness_call_cmd(op);
}

int main(int argc, char **argv)
{
	if (argc < 2) {
		puts("supply a command log to replay");
		return 1;
	}
	int fd = open(argv[1], O_RDONLY);
	if (fd < 0) {
		perror("open");
		return 1;
	}

	op_map = ht_create(0x1000);
	struct redisCommand *c;
	for (int i = 0;; i++) {
		c = &redisCommandTable[i];
		if (!c->declared_name)
			break;
		char buf[0x100];
		int j;
		for (j = 0; c->declared_name[j]; j++)
			buf[j] = toupper(c->declared_name[j]);
		ht_set(op_map, strndup(buf, j), j, i);
	}

	harness_init();
	// commands longer than sizeof(cmd) will be silently dropped
	// TODO: handle this better
	char cmd[MAX_CMD];
	char chunk[0x8000];
	ssize_t len = 0;
	for(;;) {
		ssize_t n = read(fd, chunk, sizeof(chunk));
		if (n <= 0)
			break;
		for (ssize_t i = 0; i < n; i++) {
			char c = chunk[i];
			if (c == '\n') {
				replay_command(cmd, len);
				len = 0;
			} else if (len < sizeof(cmd)-1) {
				cmd[len++] = c;
			}
		}
	}
	if (len)
		replay_command(cmd, len);
	return 0;
}*/
