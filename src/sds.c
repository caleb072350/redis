#include "sds.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include "zmalloc.h"

static void sdsOomAbort(void) {
    fprintf(stderr, "SDS: Out Of Memory (SDS_ABORT_ON_OOM defined)\n");
    abort();
}

sds sdsnewlen(const void *init, size_t initlen)
{
    struct sdshdr *sh;
    sh = zmalloc(sizeof(struct sdshdr)+initlen+1);
    if (sh == NULL) sdsOomAbort();
    sh->len = initlen;
    sh->free = 0;
    if (initlen) {
        if (init) memcpy(sh->buf, init, initlen);
        else memset(sh->buf,0,initlen);
    }
    sh->buf[initlen] = '\0';
    return (char*)sh->buf;
}

sds sdsempty(void) {
    return sdsnewlen("",0);
}

sds sdsnew(const char *init) {
    size_t initlen = (init == NULL) ? 0 : strlen(init);
    return sdsnewlen(init, initlen);
}

size_t sdslen(const sds s) {
    struct sdshdr *sh = (void*) (s-(sizeof(struct sdshdr)));
    return sh->len;
}

sds sdsdup(const sds s) {
    return sdsnewlen(s, sdslen(s));
}

void sdsfree(sds s) {
    if (s == NULL) return;
    zfree(s-sizeof(struct sdshdr));
}

size_t sdsavail(sds s) {
    struct sdshdr *sh = (void*) (s-sizeof(struct sdshdr));
    return sh->free;
}

void sdsupdatelen(sds s) {
    struct sdshdr *sh = (void*) (s-sizeof(struct sdshdr));
    int reallen = strlen(s);
    sh->free += (sh->len-reallen);
    sh->len = reallen;
}

static sds sdsMakeRoomFor(sds s, size_t addlen) {
    struct sdshdr *sh, *newsh;
    size_t free = sdsavail(s);
    size_t len, newlen;

    if (free >= addlen) return s;
    len = sdslen(s);
    sh = (void*) (s-sizeof(struct sdshdr));
    newlen = (len+addlen)*2;
    newsh = zrealloc(sh, sizeof(struct sdshdr)+newlen+1);
    if (newsh == NULL) sdsOomAbort();
    newsh->free = newlen - len;
    return newsh->buf;
}

sds sdscatlen(sds s, void *t, size_t len) {
    struct sdshdr *sh;
    size_t curlen = sdslen(s);
    s = sdsMakeRoomFor(s,len);
    if (s == NULL) return NULL;
    sh = (void*) (s-sizeof(struct sdshdr));
    memcpy(s+curlen, t, len);
    sh->len = curlen + len;
    sh->free -= len;
    s[curlen+len] = '\0';
    return s;
}

sds sdscat(sds s, char *t) {
    return sdscatlen(s, t, strlen(t));
}

sds sdscpylen(sds s, char* t, size_t len) {
    struct sdshdr *sh = (void*) (s-sizeof(struct sdshdr));
    size_t totlen = sh->free+sh->len;

    if (totlen < len) {
        s = sdsMakeRoomFor(s, len-totlen);
        if (s == NULL) return NULL;
        sh = (void*) (s-sizeof(struct sdshdr));
        totlen = sh->free+sh->len;
    }

    memcpy(s, t, len);
    s[len] = '\0';
    sh->len = len;
    sh->free = totlen - len;
    return s;
}

sds sdscpy(sds s, char *t) {
    return sdscpylen(s, t, strlen(t));
}

sds sdscatprintf(sds s, const char *fmt, ...) {
    va_list ap;
    char *buf, *t;
    size_t buflen = 32;

    while (1) {
        buf = zmalloc(buflen);
        if (buf == NULL) sdsOomAbort();
        buf[buflen-2] = '\0';
        va_start(ap,fmt);
        vsnprintf(buf, buflen, fmt, ap);
        va_end(ap);
        if (buf[buflen-2] != '\0') {
            zfree(buf);
            buflen *= 2;
            continue;
        }
        break;
    }

    t = sdscat(s, buf);
    zfree(buf);
    return t;
}


sds sdstrim(sds s, const char *cset) {
    struct sdshdr *sh = (void*) (s-sizeof(struct sdshdr));
    char *start, *end, *sp, *ep;
    size_t len;

    sp = start = s;
    ep = end = s + sdslen(s) - 1;
    while (sp <= end && strchr(cset, *sp)) sp++;
    while (ep > start && strchr(cset, *ep)) ep--;
    len = (sp > ep) ? 0 : (ep-sp)+1;
    if (sh->buf != sp) memmove(sh->buf, sp, len);
    sh->buf[len] = '\0';
    sh->free += sh->len-len;
    sh->len = len;
    return s;
}

sds sdsrange(sds s, long start, long end) {
    struct sdshdr *sh = (void*) (s-(sizeof(struct sdshdr)));
    size_t newlen, len = sdslen(s);

    if (len == 0) return 0;
    if (start < 0) {
        start = len+start;
        if (start < 0) start = 0;
    }
    if (end < 0) {
        end = end + len;
        if (end < 0) end = 0;
    }
    newlen = (start > end) ? 0 : (end-start)+1;
    if (newlen != 0) {
        if (start >= (signed)len) start = len-1;
        if (end >= (signed)len) end = len-1;
        newlen = (start > end) ? 0 : (end-start+1);
    } else {
        start = 0;
    }
    if (start != 0) memmove(sh->buf, sh->buf+start, newlen);
    sh->buf[newlen] = 0;
    sh->free = sh->free + (sh->len-newlen);
    sh->len = newlen;
    return s;
}

void sdstolower(sds s) {
    int len = sdslen(s), j;
    for (j = 0; j < len; j++) s[j] = tolower(s[j]);
}

void sdstoupper(sds s) {
    int len = sdslen(s), j;
    for (j = 0; j < len; j++) s[j] = toupper(s[j]);
}

int sdscmp(sds s1, sds s2) {
    size_t l1, l2, minlen;
    int cmp;

    l1 = sdslen(s1);
    l2 = sdslen(s2);
    minlen = (l1 < l2) ? l1 : l2;
    cmp = memcmp(s1,s2,minlen);
    if (cmp == 0) return l1-l2;
    return cmp;
}

sds *sdssplitlen(char *s, int len, char *sep, int seplen, int *count) {
    int elements = 0, slots = 5, start = 0, j;
    sds *tokens = zmalloc(sizeof(sds)*slots);
    if (tokens == NULL) sdsOomAbort();
    if (seplen < 1 || len < 0 || tokens == NULL) return NULL;
    for (j = 0; j < (len - (seplen-1)); j++) {
        if (slots < elements+2) {
            slots *=2;
            sds *newtokens = zrealloc(tokens, sizeof(sds)*slots);
            if (newtokens == NULL) sdsOomAbort();
            tokens = newtokens;
        }
        if ((seplen == 1 && *(s+j) == sep[0]) || (memcmp(s+j,sep,seplen) == 0)) {
            tokens[elements] = sdsnewlen(s+start,j-start);
            if (tokens[elements] == NULL) sdsOomAbort();
            elements++;
            start = j + seplen;
            j = j+seplen-1;
        }
    }
    tokens[elements] = sdsnewlen(s+start, len-start);
    if (tokens[elements] == NULL) sdsOomAbort();
    elements++;
    *count = elements;
    return tokens;
}
