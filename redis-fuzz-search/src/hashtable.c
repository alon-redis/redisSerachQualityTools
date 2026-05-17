#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define HT_DELETED ((const char *)1)

typedef struct { const char *key; size_t len; int val; } Entry;

typedef struct { size_t cap; size_t count; Entry *e; } Hashtable;

static uint64_t ht_hash(const char *p, size_t n) {
    uint64_t h = 14695981039346656037ULL;
    while (n--) h = (h ^ (unsigned char)*p++) * 1099511628211ULL;
    return h;
}

Hashtable *ht_create(size_t cap) {
    if (cap < 8) cap = 8;
    Hashtable *h = malloc(sizeof *h);
    h->cap = cap;
    h->count = 0;
    h->e = calloc(cap, sizeof *h->e);
    return h;
}

void ht_set(Hashtable *h, const char *k, size_t len, int v) {
    size_t i = ht_hash(k, len) % h->cap;
    while (h->e[i].key && h->e[i].key != HT_DELETED) {
        if (h->e[i].len == len && memcmp(h->e[i].key, k, len) == 0) {
            h->e[i].val = v;
            return;
        }
        i = (i + 1) % h->cap;
    }
    h->e[i].key = k;
    h->e[i].len = len;
    h->e[i].val = v;
    h->count++;
}

int ht_get(const Hashtable *h, const char *k, size_t len, int *out) {
    size_t i = ht_hash(k, len) % h->cap;
    while (h->e[i].key) {
        if (h->e[i].key != HT_DELETED && h->e[i].len == len && memcmp(h->e[i].key, k, len) == 0) {
            if (out) *out = h->e[i].val;
            return 1;
        }
        i = (i + 1) % h->cap;
    }
    return 0;
}

void ht_delete(Hashtable *h, const char *k, size_t len) {
    size_t i = ht_hash(k, len) % h->cap;
    while (h->e[i].key) {
        if (h->e[i].key != HT_DELETED && h->e[i].len == len && memcmp(h->e[i].key, k, len) == 0) {
            h->e[i].key = HT_DELETED;
            h->count--;
            return;
        }
        i = (i + 1) % h->cap;
    }
}

void ht_destroy(Hashtable *h) { free(h->e); free(h); }
