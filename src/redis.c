#define REDIS_VERSION "0.07"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <inttypes.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "ae.h"     /* Event driven programming library */
#include "sds.h"    /* Dynamic safe strings */
#include "anet.h"   /* Networking the easy way */
#include "dict.h"   /* Hash tables */
#include "adlist.h" /* Linked lists */
#include "zmalloc.h"  /* total memory usage aware version of malloc/free */

/* Error codes */
#define REDIS_OK        0
#define REDIS_ERR       -1

/* Static server configuration */
#define REDIS_SERVERPORT        6379    /* TCP port */
#define REDIS_MAXIDLETIME       (60*5)  /* Default client timeout */
#define REDIS_QUERYBUF_LEN      1024
#define REDIS_LOADBUF_LEN       1024
#define REDIS_MAX_ARGS          16
#define REDIS_DEFAULT_DBNUM     16
#define REDIS_CONFIGLINE_MAX    1024
#define REDIS_OBJFREELIST_MAX   1000000 /* Max number of objects to cache */
#define REDIS_MAX_SYNC_TIME     60      /* Slave can't take more to sync */

/* Hash table parameters */
#define REDIS_HT_MINFILL        10      /* Minimal hash table fill 10% */
#define REDIS_HT_MINSLOTS       16384   /* Never resize the HT under this */

/* Command flags */
#define REDIS_CMD_BULK          1
#define REDIS_CMD_INLINE        2

/* Object types */
#define REDIS_STRING            0
#define REDIS_LIST              1
#define REDIS_SET               2
#define REDIS_HASH              3
#define REDIS_SELECTDB          254
#define REDIS_EOF               255

/* Client flags */
#define REDIS_CLOSE             1   /* This client connection should be closed ASAP */
#define REDIS_SLAVE             2   /* This client is a slave server */
#define REDIS_MASTER            4   /* This client is a master server */

/* Server replication state */
#define REDIS_REPL_NONE      0   /* No active replication */
#define REDIS_REPL_CONNECT   1   /* Must connect to master */
#define REDIS_REPL_CONNECTED 2   /* Connected to master */

/* List releated stuff */
#define REDIS_HEAD      0
#define REDIS_TAIL      1

/* Log levels */
#define REDIS_DEBUG     0
#define REDIS_NOTICE    1
#define REDIS_WARNING   2

#define REDIS_NOTUSED(V) ((void) V)

/* ======================== Data types ===========================*/

/* A redis object, that is a type able to hold a string / list /set */
typedef struct redisObject {
    int type;
    void *ptr;
    int refcount;
} robj;

/* With multiplexing we need to take per-client state. 
 * Clients are taken in a linked list. */
typedef struct redisClient {
    int fd;
    dict *dict;
    int dictId;
    sds querybuf;
    robj *argv[REDIS_MAX_ARGS];
    int argc;
    int bulklen;    /* bulk read len. -1 if not in bulk read mode. */
    list *reply;
    int sentlen;
    time_t lastinteraction;   /* time of the last interaction, used for timeout */
    int flags;      /* REDIS_CLOSE | REDIS_SLAVE */
    int slaveseldb;  /* slave selected db, if this client is a slave. */
} redisClient;

struct saveparam {
    time_t seconds;
    int changes;
};

/* Global server state structure */
struct redisServer {
    int port;
    int fd;
    dict **dict;
    long long dirty;     /* Changes to DB from the last save */
    list *clients;
    list *slaves;
    char neterr[ANET_ERR_LEN];
    aeEventLoop *el;
    int cronloops;       /* number of times the cron function run */
    list *objfreelist;   /* A list of freed objects to avoid malloc() */
    time_t lastsave;     /* Unix time of last save succeed */
    int usedmemory;      /* Used memory in megabytes */
    /* Fields used only for stats */
    time_t stat_starttime;   /* server start time */
    long long stat_numcommands;  /* number of processed commands */
    long long stat_numconnections;  /* number of connections received */
    /* Configuration */
    int verbosity;
    int glueoutputbuf;
    int maxidletime;
    int dbnum;
    int daemonize;
    int bgsaveinprogress;
    struct saveparam *saveparams;
    int saveparamslen;
    char *logfile;
    char *bindaddr;
    char *dbfilename;
    /* Replication related */
    int isslave;
    char *masterhost;
    int masterport;
    redisClient *master;
    int replstate;
    /* Sort parameters - qsort_r() is only available under BSD so we
    * have to take this global state, in order to pass it to sortCompare() */
    int sort_desc;
    int sort_alpha;
    int sort_bypattern;
};

typedef void redisCommandProc(redisClient *c);
struct redisCommand {
    char *name;
    redisCommandProc *proc;
    int arity;
    int flags;
};

typedef struct _redisSortObject {
    robj *obj;
    union {
        double score;
        robj *cmpobj;
    } u;
} redisSortObject;

typedef struct _redisSortOperation {
    int type;
    robj *pattern;
} redisSortOperation;

struct sharedObjectsStruct {
    robj *crlf, *ok, *err, *zerobulk, *nil, *zero, *one, *pong, *space,
    *minus1, *minus2, *minus3, *minus4, *wrongtypeerr, *nokeyerr,
    *wrongtypeerrbulk, *nokeyerrbulk, *syntaxerr, *syntaxerrbulk,
    *select0, *select1, *select2, *select3, *select4, *select5,
    *select6, *select7, *select8, *select9;
} shared;

/*===================================== Prototypes ========================================*/

static void freeStringObject(robj *o);
static void freeListObject(robj *o);
static void freeSetObject(robj *o);
static void decrRefCount(void *o);
static robj *createObject(int type, void *ptr);
static void freeClient(redisClient *c);
static int loadDb(char *filename);
static void addReply(redisClient *c, robj *obj);
static void addReplySds(redisClient *c, sds s);
static void incrRefCount(robj *o);
static int saveDbBackground(char *filename);
static robj *createStringObject(char *ptr, size_t len);
// static void replicationFeedSlaves(struct redisCommand *cmd, int dictid, robj **argv, int argc);
static int syncWithMaster(void);

static void pingCommand(redisClient *c);
static void echoCommand(redisClient *c);
static void setCommand(redisClient *c);
static void setnxCommand(redisClient *c);
static void getCommand(redisClient *c);
// static void delCommand(redisClient *c);
// static void existsCommand(redisClient *c);
// static void incrCommand(redisClient *c);
// static void decrCommand(redisClient *c);
// static void incrbyCommand(redisClient *c);
// static void decrbyCommand(redisClient *c);
// static void selectCommand(redisClient *c);
// static void randomkeyCommand(redisClient *c);
// static void keysCommand(redisClient *c);
// static void dbsizeCommand(redisClient *c);
// static void lastsaveCommand(redisClient *c);
// static void saveCommand(redisClient *c);
// static void bgsaveCommand(redisClient *c);
// static void shutdownCommand(redisClient *c);
// static void moveCommand(redisClient *c);
// static void renameCommand(redisClient *c);
// static void renamenxCommand(redisClient *c);
// static void lpushCommand(redisClient *c);
// static void rpushCommand(redisClient *c);
// static void lpopCommand(redisClient *c);
// static void rpopCommand(redisClient *c);
// static void llenCommand(redisClient *c);
// static void lindexCommand(redisClient *c);
// static void lrangeCommand(redisClient *c);
// static void ltrimCommand(redisClient *c);
// static void typeCommand(redisClient *c);
// static void lsetCommand(redisClient *c);
// static void saddCommand(redisClient *c);
// static void sremCommand(redisClient *c);
// static void sismemberCommand(redisClient *c);
// static void scardCommand(redisClient *c);
// static void sinterCommand(redisClient *c);
// static void sinterstoreCommand(redisClient *c);
// static void syncCommand(redisClient *c);
// static void flushdbCommand(redisClient *c);
// static void flushallCommand(redisClient *c);
// static void sortCommand(redisClient *c);
// static void lremCommand(redisClient *c);
// static void infoCommand(redisClient *c);

/*===================================== Globals ====================================*/

/* Global vars */
static struct redisServer server;  /* server global state */
static struct redisCommand cmdTable[] = {
    {"get",getCommand,2,REDIS_CMD_INLINE},
    {"set",setCommand,3,REDIS_CMD_BULK},
    {"setnx",setnxCommand,3,REDIS_CMD_BULK},
    // {"del",delCommand,2,REDIS_CMD_INLINE},
    // {"exists",existsCommand,2,REDIS_CMD_INLINE},
    // {"incr",incrCommand,2,REDIS_CMD_INLINE},
    // {"decr",decrCommand,2,REDIS_CMD_INLINE},
    // {"rpush",rpushCommand,3,REDIS_CMD_BULK},
    // {"lpush",lpushCommand,3,REDIS_CMD_BULK},
    // {"rpop",rpopCommand,2,REDIS_CMD_INLINE},
    // {"lpop",lpopCommand,2,REDIS_CMD_INLINE},
    // {"llen",llenCommand,2,REDIS_CMD_INLINE},
    // {"lindex",lindexCommand,3,REDIS_CMD_INLINE},
    // {"lset",lsetCommand,4,REDIS_CMD_BULK},
    // {"lrange",lrangeCommand,4,REDIS_CMD_INLINE},
    // {"ltrim",ltrimCommand,4,REDIS_CMD_INLINE},
    // {"lrem",lremCommand,4,REDIS_CMD_BULK},
    // {"sadd",saddCommand,3,REDIS_CMD_BULK},
    {NULL,NULL,0,0}
};

/* ================================ Utility functions ================================*/

/* Glob style pattern matching. */
int stringmatchlen(const char *pattern, int patternLen, const char *string, int stringlen, int nocase)
{
    while(patternLen) {
        switch (pattern[0])
        {
        case '*':
            while(pattern[1] == '*') {
                pattern++;
                patternLen--;
            }
            if (patternLen == 1)
                return 1;  /* match */
            while (stringlen) {
                if (stringmatchlen(pattern+1, patternLen-1, string, stringlen, nocase))
                    return 1; /* match */
                string++;
                stringlen--;
            }
            return 0; /* no match */
            break;
        case '?':
            if (stringlen == 0) return 0; /* no match */
            string++;
            stringlen--;
            break;
        case '[':
        {
            int not, match;
            pattern++;
            patternLen--;
            not = pattern[0] == '^';
            if (not) {
                pattern++;
                patternLen--;
            }
            match = 0;
            while(1) {
                if (pattern[0] == '\\') {
                    pattern++;
                    patternLen--;
                    if (pattern[0] == string[0])
                        match = 1;
                } else if (pattern[0] == ']') {
                    break;
                } else if (patternLen == 0) {
                    pattern--;
                    patternLen++;
                    break;
                } else if (pattern[1] == '-' && patternLen >= 3) {
                    int start = pattern[0];
                    int end = pattern[2];
                    int c = string[0];
                    if (start > end) {
                        int t = start;
                        start = end;
                        end = t;
                    }
                    if (nocase) {
                        start = tolower(start);
                        end = tolower(end);
                        c = tolower(c);
                    }
                    pattern += 2;
                    patternLen -= 2;
                    if (c >= start && c <= end) 
                        match = 1;
                } else {
                    if (!nocase) {
                        if (pattern[0] == string[0])
                            match = 1;
                    } else {
                        if (tolower((int)pattern[0]) == tolower((int)string[0]))
                            match = 1;
                    }
                }
                pattern++;
                patternLen--;
            }
            if (not)
                match = !match;
            if (!match) return 0; /* no match */
            string++;
            stringlen--;
            break;
        } 
        case '\\':
            if (patternLen >= 2) {
                pattern++;
                patternLen--;
            }
            /* fall through */
        default:
            if (!nocase) {
                if (pattern[0] != string[0])
                    return 0; /* no match */
            } else {
                if (tolower((int)pattern[0]) != tolower((int)string[0]))
                    return 0; /* no match */
            }
            string++;
            stringlen--;
            break;
        }
        pattern++;
        patternLen--;
        if (stringlen == 0) {
            while(*pattern == '*') {
                pattern++;
                patternLen--;
            }
            break;
        }
    }
    if (patternLen == 0 && stringlen == 0)
        return 1;
    return 0;
}

void redisLog(int level, const char *fmt, ...)
{
    va_list ap;
    FILE *fp;

    fp = (server.logfile == NULL) ? stdout : fopen(server.logfile, "a");
    if (!fp) return;
    
    va_start(ap, fmt);
    if (level >= server.verbosity) {
        char *c = ".-*";
        fprintf(fp,"%c ", c[level]);
        vfprintf(fp, fmt, ap);
        fprintf(fp, "\n");
        fflush(fp);
    }
    va_end(ap);

    if (server.logfile) fclose(fp);
}

/* ================================ Hash table type implementation ===========================*/

/** This is an hash table type that uses the SDS dynamic strings library as
 * keys and redis objects as values (objects can be SDS strings, lists, sets.)
*/

static int sdsDictKeyCompare(void *privdata, const void *key1, const void *key2)
{
    int l1, l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

static void dictRedisObjectDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    decrRefCount(val);
}

static int dictSdsKeyCompare(void *privdata, const void* key1, const void *key2)
{
    const robj *o1 = key1, *o2 = key2;
    return sdsDictKeyCompare(privdata, o1->ptr, o2->ptr);
}

static unsigned int dictSdsHash(const void *key) {
    const robj *o = key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

static dictType setDictType = {
    dictSdsHash,                /* Hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictRedisObjectDestructor,  /* key destructor */
    NULL
};

static dictType hashDictType = {
    dictSdsHash,                 /* hash function */
    NULL,                        /* key dup */
    NULL,                        /* val dup */
    dictSdsKeyCompare,           /* key compare */
    dictRedisObjectDestructor,   /* key destructor */
    dictRedisObjectDestructor    /* val destructor */
};

/* ================================ Random utility functions ==============================*/

/* Redis generally does not try to recover from out of memory conditions when 
 * allocating objects or strings, it is not clear if it will be possible to report
 * this condition to the client since the networking layer itself is based on heap
 * allocation for send buffers, so we simply abort. At least the code will be simpler
 * to read ... */
static void oom(const char *msg) {
    fprintf(stderr, "%s: Out of memory\n", msg);
    fflush(stderr);
    sleep(1);
    abort();
}


/* =============================== Redis server networking stuff ============================*/

void closeTimeoutClients(void) {
    redisClient *c;
    listIter *li;
    listNode *ln;
    time_t now = time(NULL);

    li = listGetIterator(server.clients, AL_START_HEAD);
    if (!li) return;
    while ((ln = listNextElement(li)) != NULL) {
        c = listNodeValue(ln);
        if (!(c->flags & REDIS_SLAVE) &&  // no timeout for slaves
            (now - c->lastinteraction > server.maxidletime)) {
                redisLog(REDIS_DEBUG, "Closing idle client");
                freeClient(c);
            }
    }
    listReleaseIterator(li);
}

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
    int j, size, used, loops = server.cronloops++;
    REDIS_NOTUSED(eventLoop);
    REDIS_NOTUSED(id);
    REDIS_NOTUSED(clientData);

    /* Update the global state with the amount of used memory */
    server.usedmemory = zmalloc_used_memory();

    /* If the percentage of used slots in the HT reaches REDIS_HT_MINFILL
     * we resize the hash table to save memory */
    for (j = 0; j < server.dbnum; j++) {
        size = dictGetHashTableSize(server.dict[j]);
        used = dictGetHashTableUsed(server.dict[j]);
        if (!(loops % 5) && used > 0) {
            redisLog(REDIS_DEBUG, "DB %d : %d keys in %d slots HT.", j, used, size);
            // dictPrintStats(server.dict);
        }
        if (size && used && size > REDIS_HT_MINSLOTS &&
            (used*100/size < REDIS_HT_MINFILL)) {
            redisLog(REDIS_NOTICE,"The hash table %d is too sparse, resize it...", j);
            dictResize(server.dict[j]);
            redisLog(REDIS_NOTICE,"Hash table %d resized.", j);
        }
    }

    /* Show information about connected clients */
    if (!(loops % 5)) {
        redisLog(REDIS_DEBUG, "%d clients connected (%d slaves), %d bytes in use",
            listLength(server.clients)-listLength(server.slaves), listLength(server.slaves),
            server.usedmemory);
    }

    /* Close connections of timeout clients */
    if (!(loops % 10)) {
        closeTimeoutClients();
    }

    /* Check if a background saving in progress terminated */
    if (server.bgsaveinprogress) {
        int statloc;
        if (wait4(-1,&statloc,WNOHANG,NULL)) {
            int exitcode = WEXITSTATUS(statloc);
            if (exitcode == 0) {
                redisLog(REDIS_NOTICE, "Background saving terminated with success");
                server.dirty = 0;
                server.lastsave = time(NULL);
            } else {
                redisLog(REDIS_WARNING, "Background saving error");
            }
            server.bgsaveinprogress = 0;
        }
    } else {
        /* If there is not a background saving in progress check if 
         * we have to save now. */
        time_t now = time(NULL);
        for (j = 0; j < server.saveparamslen; j++)
        {
            struct saveparam *sp = server.saveparams+j;
            if (server.dirty >= sp->changes && now-server.lastsave > sp->seconds) {
                redisLog(REDIS_NOTICE, "%d changes in %d seconds. Saving...",
                    sp->changes, sp->seconds);
                saveDbBackground(server.dbfilename);
                break;
            }
        }
    }
    /* Check if we should connect to a MASTER */
    if (server.replstate == REDIS_REPL_CONNECT) {
        redisLog(REDIS_NOTICE, "Connecting to MASTER...");
        if (syncWithMaster() == REDIS_OK) {
            redisLog(REDIS_NOTICE, "MASTER <-> SLAVE sync succeeded");
        }
    }
    return 1000;
}


static void createSharedObjects(void) {
    shared.crlf = createObject(REDIS_STRING, sdsnew("\r\n"));
    shared.ok = createObject(REDIS_STRING, sdsnew("+OK\r\n"));
    shared.err = createObject(REDIS_STRING, sdsnew("-ERR\r\n"));
    shared.zerobulk = createObject(REDIS_STRING, sdsnew("0\r\n\r\n"));
    shared.nil = createObject(REDIS_STRING, sdsnew("nil\r\n"));
    shared.zero = createObject(REDIS_STRING, sdsnew("0\r\n"));
    shared.one = createObject(REDIS_STRING, sdsnew("1\r\n"));
    /* no such key */
    shared.minus1 = createObject(REDIS_STRING, sdsnew("-1\r\n"));
    /* operation against key holding a value of the wrong type */
    shared.minus2 = createObject(REDIS_STRING, sdsnew("-2\r\n"));
    /* src and dst objects are the same */
    shared.minus3 = createObject(REDIS_STRING, sdsnew("-3\r\n"));
    /* out of range argument */
    shared.minus4 = createObject(REDIS_STRING, sdsnew("-4\r\n"));
    shared.pong = createObject(REDIS_STRING, sdsnew("+PONG\r\n"));
    shared.wrongtypeerr = createObject(REDIS_STRING,sdsnew("-ERR Operation against a key holding the wrong kind of value\r\n"));
    shared.wrongtypeerrbulk = createObject(REDIS_STRING, sdscatprintf(sdsempty(),"%d\r\n%s", -sdslen(shared.wrongtypeerr->ptr)+2,
            shared.wrongtypeerr->ptr));
    shared.nokeyerr = createObject(REDIS_STRING, sdsnew("-ERR no such key\r\n"));
    shared.nokeyerrbulk = createObject(REDIS_STRING,sdscatprintf(sdsempty(),"%d\r\n%s", -sdslen(shared.nokeyerr->ptr)+2,shared.nokeyerr->ptr));
    shared.syntaxerr = createObject(REDIS_STRING,sdsnew("-ERR syntax error\r\n"));
    shared.syntaxerrbulk = createObject(REDIS_STRING,sdscatprintf(sdsempty(),"%d\r\n%s",-sdslen(shared.syntaxerr->ptr)+2,shared.syntaxerr->ptr));
    shared.space = createObject(REDIS_STRING,sdsnew(" "));
    shared.select0 = createStringObject("select 0\r\n", 10);
    shared.select1 = createStringObject("select 1\r\n", 10);
    shared.select2 = createStringObject("select 2\r\n", 10);
    shared.select3 = createStringObject("select 3\r\n", 10);
    shared.select4 = createStringObject("select 4\r\n", 10);
    shared.select5 = createStringObject("select 5\r\n", 10);
    shared.select6 = createStringObject("select 6\r\n", 10);
    shared.select7 = createStringObject("select 7\r\n", 10);
    shared.select8 = createStringObject("select 8\r\n", 10);
    shared.select9 = createStringObject("select 9\r\n", 10);
}

static void appendServerSaveParam(time_t seconds, int changes) {
    server.saveparams = zrealloc(server.saveparams,sizeof(struct saveparam)*(server.saveparamslen+1));
    if (server.saveparams == NULL) oom("appendServerSaveParams");
    server.saveparams[server.saveparamslen].seconds = seconds;
    server.saveparams[server.saveparamslen].changes = changes;
    server.saveparamslen++;
}

static void ResetServerSaveParams() {
    zfree(server.saveparams);
    server.saveparams = NULL;
    server.saveparamslen = 0;
}

static void initServerConfig() {
    server.dbnum = REDIS_DEFAULT_DBNUM;
    server.port = REDIS_SERVERPORT;
    server.verbosity = REDIS_DEBUG;
    server.maxidletime = REDIS_MAXIDLETIME;
    server.saveparams = NULL;
    server.logfile = NULL;
    server.bindaddr = NULL;
    server.glueoutputbuf = 1;
    server.daemonize = 0;
    server.dbfilename = "dump.rdb";
    ResetServerSaveParams();

    appendServerSaveParam(60*60,1);
    appendServerSaveParam(300,100);
    appendServerSaveParam(60,10000);

    /* Replication related */
    server.isslave = 0;
    server.masterhost = NULL;
    server.masterport = 6379;
    server.master = NULL;
    server.replstate = REDIS_REPL_NONE;
}

static void initServer() {
    int j;

    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    server.clients = listCreate();
    server.slaves = listCreate();
    server.objfreelist = listCreate();
    createSharedObjects();
    server.el = aeCreateEventLoop();
    server.dict = zmalloc(sizeof(dict*)*server.dbnum);
    if (!server.dict || !server.clients || !server.slaves || !server.el || !server.objfreelist)
        oom("server initialization");
    server.fd = anetTcpServer(server.neterr, server.port, server.bindaddr);
    if (server.fd == -1) {
        redisLog(REDIS_WARNING, "Opening TCP port: %s", server.neterr);
        exit(1);
    }

    for (j = 0; j < server.dbnum; j++) {
        server.dict[j] = dictCreate(&hashDictType, NULL);
        if (!server.dict[j])
            oom("dictCreate");
    }

    server.cronloops = 0;
    server.bgsaveinprogress = 0;
    server.lastsave = time(NULL);
    server.dirty = 0;
    server.usedmemory = 0;
    server.stat_numcommands = 0;
    server.stat_numconnections = 0;
    server.stat_starttime = time(NULL);
    aeCreateTimeEvent(server.el, 1000, serverCron, NULL, NULL);
}


/* Empty the whole database */
static void emptyDb() {
    int j;
    for (j = 0; j < server.dbnum; j++)
        dictEmpty(server.dict[j]);
}

static void loadServerConfig(char *filename) {
    FILE *fp = fopen(filename, "r");
    char buf[REDIS_CONFIGLINE_MAX+1], *err = NULL;
    int linenum = 0;
    sds line = NULL;

    if (!fp) {
        redisLog(REDIS_WARNING, "Fatal error, can't open config file");
        exit(1);
    }
    while (fgets(buf,REDIS_CONFIGLINE_MAX+1,fp) != NULL) {
        sds *argv;
        int argc, j;

        linenum++;
        line = sdsnew(buf);
        line = sdstrim(line, " \t\r\n");

        /* Skip comments and blank lines */
        if (line[0] == '#' || line[0] == '\0') {
            sdsfree(line);
            continue;
        }

        /* Split into arguments */
        argv = sdssplitlen(line,sdslen(line)," ",1,&argc);
        sdstolower(argv[0]);

        /* Execute config directives */
        if (!strcmp(argv[0],"timeout") && argc == 2) {
            server.maxidletime = atoi(argv[1]);
            if (server.maxidletime < 1) {
                err = "Invalid timeout value"; goto loaderr;
            }
        } else if (!strcmp(argv[0], "port") && argc == 2) {
            server.port = atoi(argv[1]);
            if (server.port < 1 || server.port > 65535) {
                err = "Invalid port"; goto loaderr;
            }
        } else if (!strcmp(argv[0], "bind") && argc == 2) {
            server.bindaddr = zstrdup(argv[1]);
        }
        else if (!strcmp(argv[0], "save") && argc == 3) {
            int seconds = atoi(argv[1]);
            int changes = atoi(argv[2]);
            if (seconds < 1 || changes < 0) {
                err = "Invalid save paramters"; goto loaderr;
            }
            appendServerSaveParam(seconds,changes);
        } else if (!strcmp(argv[0], "dir") && argc == 2) {
            if (chdir(argv[1]) == -1) {
                redisLog(REDIS_WARNING, "Can't chdir to '%s': %s", argv[1], strerror(errno));
                exit(1);
            }
        } else if (!strcmp(argv[0], "loglevel") && argc == 2) {
            if (!strcmp(argv[1],"debug")) server.verbosity = REDIS_DEBUG;
            else if (!strcmp(argv[1],"notice")) server.verbosity = REDIS_NOTICE;
            else if (!strcmp(argv[1],"warning")) server.verbosity = REDIS_WARNING;
            else {
                err = "Invalid log level. Must be one of debug, notice, warning";
                goto loaderr;
            }
        } else if (!strcmp(argv[0],"logfile") && argc == 2) {
            FILE *fp;

            server.logfile = zstrdup(argv[1]);
            if (!strcmp(server.logfile,"stdout")) {
                zfree(server.logfile);
                server.logfile = NULL;
            }
            if (server.logfile) {
                /* Test if we are able to open the file. The server will not
                 * be able to abort just for this problem later... */
                fp = fopen(server.logfile,"a");
                if (fp == NULL) {
                    err = sdscatprintf(sdsempty(), "Can't open the log file: %s", strerror(errno));
                    goto loaderr;
                }
                fclose(fp);
            }
        } else if (!strcmp(argv[0], "databases") && argc == 2) {
            server.dbnum = atoi(argv[1]);
            if (server.dbnum < 1) {
                err = "Invalid number of databases"; goto loaderr;
            }
        } else if (!strcmp(argv[0],"slaveof") && argc == 3) {
            server.masterhost = sdsnew(argv[1]);
            server.masterport = atoi(argv[2]);
            server.replstate = REDIS_REPL_CONNECT;
        } else if (!strcmp(argv[0], "glueoutputbuf") && argc == 2) {
            sdstolower(argv[1]);
            if (!strcmp(argv[1],"yes")) server.glueoutputbuf = 1;
            else if (!strcmp(argv[1],"no")) server.glueoutputbuf = 0;
            else {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcmp(argv[0],"daemonize") && argc == 2) {
            sdstolower(argv[1]);
            if (!strcmp(argv[1],"yes")) server.daemonize = 1;
            else if (!strcmp(argv[1],"no")) server.daemonize = 0;
            else {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else {
            err = "Bad directive or wrong number of arguments"; goto loaderr;
        }
        for (j = 0; j < argc; j++) {
            sdsfree(argv[j]);
        }
        zfree(argv);
        sdsfree(line);
    }
    fclose(fp);
    return;

loaderr:
    fprintf(stderr, "\n*** FATAL CONFIG FILE ERROR ***\n");
    fprintf(stderr, "Reading the configuration file, at line %d\n", linenum);
    fprintf(stderr, ">>> '%s'\n", line);
    fprintf(stderr, "%s\n", err);
    exit(1);
}



static void freeClientArgv(redisClient *c) {
    int j;
    for (j = 0; j < c->argc; j++) {
        decrRefCount(c->argv[j]);
    }
    c->argc = 0;
}

static void freeClient(redisClient *c) {
    listNode *ln;
    
    aeDeleteFileEvent(server.el, c->fd, AE_READABLE);
    aeDeleteFileEvent(server.el, c->fd, AE_WRITABLE);
    sdsfree(c->querybuf);
    listRelease(c->reply);
    freeClientArgv(c);
    close(c->fd);
    ln = listSearchKey(server.clients, c);
    assert(ln != NULL);
    listDelNode(server.clients, ln);
    if (c->flags & REDIS_SLAVE) {
        ln = listSearchKey(server.slaves, c);
        assert(ln != NULL);
        listDelNode(server.slaves,ln);
    }
    if (c->flags & REDIS_MASTER) {
        server.master = NULL;
        server.replstate = REDIS_REPL_CONNECT;
    }
    zfree(c);
}

static void glueReplyBuffersIfNeeded(redisClient *c)
{
    int totlen = 0;
    listNode *ln = c->reply->head, *next;
    robj *o;

    while (ln) {
        o = ln->value;
        totlen += sdslen(o->ptr);
        ln = ln->next;
        /* This optimization makes more sence if we don't have to copy
         * too much data. */
        if (totlen > 1024) return;
    }
    if (totlen > 0) {
        char buf[1024];
        int copylen = 0;
        ln = c->reply->head;
        while (ln) {
            next = ln->next;
            o = ln->value;
            memcpy(buf+copylen,o->ptr,sdslen(o->ptr));
            copylen += sdslen(o->ptr);
            listDelNode(c->reply,ln);
            ln = next;
        }
        /* Now the output buffer is empty, add the new single element */
        addReplySds(c,sdsnewlen(buf,totlen));
    }
}

static void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask)
{
    redisClient *c = privdata;
    int nwritten = 0, totwritten = 0, objlen;
    robj *o;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    if (server.glueoutputbuf && listLength(c->reply) > 1)
        glueReplyBuffersIfNeeded(c);
    while (listLength(c->reply)) {
        o = listNodeValue(listFirst(c->reply));
        objlen = sdslen(o->ptr);

        if (objlen == 0) {
            listDelNode(c->reply, listFirst(c->reply));
            continue;
        }

        if (c->flags & REDIS_MASTER) {
            nwritten = objlen - c->sentlen;
        }  else {
            nwritten = write(fd, o->ptr+c->sentlen, objlen - c->sentlen);
            if (nwritten <= 0) break;
        }
        c->sentlen += nwritten;
        totwritten += nwritten;
        /* If we fully sent the object on head go to the next one */
        if (c->sentlen == objlen) {
            listDelNode(c->reply, listFirst(c->reply));
            c->sentlen = 0;
        }
    }
    if (nwritten == -1) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } else {
            redisLog(REDIS_DEBUG, "Error writing to client: %s", strerror(errno));
            freeClient(c);
            return;
        }
    }
    if (totwritten > 0) c->lastinteraction = time(NULL);
    if (listLength(c->reply) == 0) {
        c->sentlen = 0;
        aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
    }
}

static struct redisCommand *lookupCommand(char *name) {
    int j = 0;
    while (cmdTable[j].name != NULL) {
        if (!strcmp(name,cmdTable[j].name)) return &cmdTable[j];
        j++;
    }
    return NULL;
}

/* resetClient prepare the client to process the next command */
static void resetClient(redisClient *c) {
    freeClientArgv(c);
    c->bulklen = -1;
}

/* If this function gets called we already read a whole
 * command, arguments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the server
 * for a bulk read from the client.
 * 
 * If 1 is returned the client is still alive and valid and
 * other operation can be performed by the caller. Otherwise
 * if 0 is returned the client was destroyed (i.e. after QUIT) */
static int processCommand(redisClient *c) {
    struct redisCommand *cmd;
    long long dirty;

    sdstolower(c->argv[0]->ptr);
    /* The QUIT command is handled as a special case. Normal command
     * procs are unable to close the client connection safely. */
    if (!strcmp(c->argv[0]->ptr, "quit")) {
        freeClient(c);
        return 0;
    }

    cmd = lookupCommand(c->argv[0]->ptr);
    if (!cmd) {
        addReplySds(c,sdsnew("-ERR unknown command\r\n"));
        resetClient(c);
        return 1;
    } else if ((cmd->arity > 0 && cmd->arity != c->argc) ||
            (c->argc < -cmd->arity)) {
        addReplySds(c,sdsnew("-ERR wrong number of arguments\r\n"));
        resetClient(c);
        return 1;
    } else if (cmd->flags & REDIS_CMD_BULK && c->bulklen == -1) {
        int bulklen = atoi(c->argv[c->argc-1]->ptr);

        decrRefCount(c->argv[c->argc-1]);
        if (bulklen < 0 || bulklen > 1024*1024*1024) {
            c->argc--;
            addReplySds(c,sdsnew("-ERR invalid bulk write count\r\n"));
            resetClient(c);
            return 1;
        }
        c->argc--;
        c->bulklen = bulklen+2; /* add two bytes for CR+LF */
        /* It is possible that the bulk read is already in the 
         * buffer. Check this condition and handle it accordingly */
        if ((signed)sdslen(c->querybuf) >= c->bulklen) {
            c->argv[c->argc] = createStringObject(c->querybuf,c->bulklen-2);
            c->argc++;
            c->querybuf = sdsrange(c->querybuf,c->bulklen,-1);
        } else {
            return 1;
        }
    }
    /* Exec the command */
    dirty = server.dirty;
    cmd->proc(c);
    if (server.dirty-dirty != 0 && listLength(server.slaves))
        // replicationFeedSlaves(cmd,c->dictId,c->argv,c->argc);
    server.stat_numcommands++;

    /* Prepare the client for the next command */
    if (c->flags & REDIS_CLOSE) {
        freeClient(c);
        return 0;
    }
    resetClient(c);
    return 1;
}

static void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *c = (redisClient*) privdata;
    char buf[REDIS_QUERYBUF_LEN];
    int nread;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    nread = read(fd, buf, REDIS_QUERYBUF_LEN);
    if (nread == -1) {
        if (errno == EAGAIN) 
            nread = 0;
        else {
            redisLog(REDIS_DEBUG, "Reading from client: %s", strerror(errno));
            freeClient(c);
            return;
        }
    } else if (nread == 0) {
        redisLog(REDIS_DEBUG, "Client closed connection");
        freeClient(c);
        return;
    }
    if (nread) {
        c->querybuf = sdscatlen(c->querybuf, buf, nread);
        c->lastinteraction = time(NULL);
    } else {
        return;
    }

again:
    if (c->bulklen == -1) {
        /* Read the first line of the query */
        char *p = strchr(c->querybuf, '\n');
        size_t querylen;
        if (p) {
            sds query, *argv;
            int argc, j;

            query = c->querybuf;
            c->querybuf = sdsempty();
            querylen = 1+(p-query);
            if (sdslen(query) > querylen) {
                /* leave data after the first line of the query in the buffer */
                c->querybuf = sdscatlen(c->querybuf,query+querylen,sdslen(query)-querylen);
            }
            *p = '\0';  /* remove '\n' */
            if (*(p-1) == '\r') *(p-1) = '\0'; /* and '\r' if any */
            sdsupdatelen(query);

            /* Now we can split the query in arguments */
            if (sdslen(query) == 0) {
                /* Ignore empty query */
                sdsfree(query);
                return;
            }

            argv = sdssplitlen(query,sdslen(query)," ", 1, &argc);
            sdsfree(query);
            if (argv == NULL ) oom("sdssplitlen");
            for (j = 0; j < argc && j < REDIS_MAX_ARGS; j++) {
                if (sdslen(argv[j])) {
                    c->argv[c->argc] = createObject(REDIS_STRING,argv[j]);
                    c->argc++;
                } else {
                    sdsfree(argv[j]);
                }
            }
            zfree(argv);
            /* Execute the command. If the client is still valid
             * after processCommand() return and there is something
             * on the query buffer try to process the next command. */
            if (processCommand(c) && sdslen(c->querybuf)) goto again;
            return;
        } else if (sdslen(c->querybuf) > 1024) {
            redisLog(REDIS_DEBUG, "Client protocol error");
            freeClient(c);
            return;
        }
    } else {
        /* Bulk read handling. Note that if we are at this point
         * the client already sent a command terminated with a newline
         * we are reading the bulk data that is actually the last
         * argument of the command. */
        int qbl = sdslen(c->querybuf);

        if (c->bulklen <= qbl) {
            /* Copy everything but the final CRLF as final argument */
            c->argv[c->argc] = createStringObject(c->querybuf, c->bulklen-2);
            c->argc++;
            c->querybuf = sdsrange(c->querybuf,c->bulklen,-1);
            processCommand(c);
            return;
        }
    }
}

static int selectDb(redisClient *c, int id) {
    if (id < 0 || id >= server.dbnum) {
        return REDIS_ERR;
    }
    c->dict = server.dict[id];
    c->dictId = id;
    return REDIS_OK;
}

static redisClient* createClient(int fd) {
    redisClient *c = zmalloc(sizeof(*c));

    anetNonBlock(NULL, fd);
    anetTcpNoDelay(NULL, fd);
    if (!c) return NULL;
    selectDb(c,0);
    c->fd = fd;
    c->querybuf = sdsempty();
    c->argc = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->flags = 0;
    c->lastinteraction = time(NULL);
    if ((c->reply = listCreate()) == NULL) oom("listCreate");
    listSetFreeMethod(c->reply,decrRefCount);
    if (aeCreateFileEvent(server.el, c->fd, AE_READABLE, readQueryFromClient, c, NULL) == AE_ERR) {
        freeClient(c);
        return NULL;
    }
    if (!listAddNodeTail(server.clients,c)) oom("listAddNodeTail");
    return c;
}

static void addReply(redisClient *c, robj *obj) 
{
    if (listLength(c->reply) == 0 &&
        aeCreateFileEvent(server.el, c->fd, AE_WRITABLE,
            sendReplyToClient, c, NULL) == AE_ERR) return;
    if (!listAddNodeTail(c->reply, obj)) oom("listAddNodeTail");
    incrRefCount(obj);
}

static void addReplySds(redisClient *c, sds s) {
    robj *o = createObject(REDIS_STRING, s);
    addReply(c,o);
    decrRefCount(o);
}

static void acceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd;
    char cip[128];
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    REDIS_NOTUSED(privdata);

    cfd = anetAccept(server.neterr, fd, cip, &cport);
    if (cfd == AE_ERR) {
        redisLog(REDIS_DEBUG, "Accepting client connection: %s", server.neterr);
        return;
    }
    redisLog(REDIS_DEBUG,"Accepted %s:%d", cip, cport);
    if (createClient(cfd) == NULL) {
        redisLog(REDIS_WARNING, "Error allocating resources for the client.");
        close(cfd); /* May be already closed, just ignore errors */
        return;
    }
    server.stat_numconnections++;
}

/* ======================== Redis objects implementation ==========================*/

static robj *createObject(int type, void *ptr) {
    robj *o;
    if (listLength(server.objfreelist)) {
        listNode *head = listFirst(server.objfreelist);
        o = listNodeValue(head);
        listDelNode(server.objfreelist,head);
    } else {
        o = zmalloc(sizeof(*o));
    }
    if (!o) oom("createObject");
    o->type = type;
    o->ptr = ptr;
    o->refcount = 1;
    return o;
}

static robj *createStringObject(char *ptr, size_t len) {
    return createObject(REDIS_STRING, sdsnewlen(ptr,len));
}

static robj *createListObject(void) {
    list *l = listCreate();
    if (!l) oom("listCreate");
    listSetFreeMethod(l,decrRefCount);
    return createObject(REDIS_LIST,l);
}

static robj *createSetObject(void) {
    dict *d = dictCreate(&setDictType, NULL);
    if (!d) oom("dictCreate");
    return createObject(REDIS_SET,d);
}


static void freeStringObject(robj *o) {
    sdsfree(o->ptr);
}

static void freeListObject(robj *o) {
    listRelease((list*) o->ptr);
}

static void freeSetObject(robj *o) {
    dictRelease((dict*) o->ptr);
}

static void freeHashObject(robj *o) {
    dictRelease((dict*) o->ptr);
}

static void incrRefCount(robj *o) {
    o->refcount++;
}

static void decrRefCount(void *obj) {
    robj *o = obj;
    if (--(o->refcount) == 0) {
        switch (o->type)
        {
        case REDIS_STRING:
            freeStringObject(o);
            break;
        case REDIS_LIST:
            freeListObject(o);
            break;
        case REDIS_SET:
            freeSetObject(o);
            break;
        case REDIS_HASH:
            freeHashObject(o);
            break;
        default:
            assert(0 != 0);
            break;
        }
        if (listLength(server.objfreelist) > REDIS_OBJFREELIST_MAX ||
            !listAddNodeHead(server.objfreelist,o))
            zfree(o);
    }
}


/* ========================= DB saving/loading ================================*/

/* Save the DB on disk. Return REDIS_ERR on error, REDIS_OK on success */
static int saveDb(char *filename) {
    dictIterator *di = NULL;
    dictEntry *de;
    uint32_t len;
    uint8_t type;
    FILE *fp;
    char tmpfile[256];
    int j;

    snprintf(tmpfile, 256, "tmp-%d.%ld.rdb",(int)time(NULL),(long int)random());
    fp = fopen(tmpfile,"w");
    if (!fp) {
        redisLog(REDIS_WARNING, "Failed saving the DB: %s", strerror(errno));
        return REDIS_ERR;
    }
    if (fwrite("REDIS0000",9,1,fp) == 0) goto werr;
    for (j = 0; j < server.dbnum; j++) {
        dict *d = server.dict[j];
        if (dictGetHashTableUsed(d) == 0) continue;
        di = dictGetIterator(d);
        if (!di) {
            fclose(fp);
            return REDIS_ERR;
        }

        /* Write the SELECT DB opcode */
        type = REDIS_SELECTDB;
        len = htonl(j);
        if (fwrite(&type,1,1,fp) == 0) goto werr;
        if (fwrite(&len,4,1,fp) == 0) goto werr;

        /* Iterate this DB writing every entry */
        while ((de = dictNext(di)) != NULL) {
            robj *key = dictGetEntryKey(de);
            robj *o = dictGetEntryVal(de);

            type = o->type;
            len = htonl(sdslen(key->ptr));
            if (fwrite(&type,1,1,fp) == 0) goto werr;
            if (fwrite(&len,4,1,fp) == 0) goto werr;
            if (fwrite(key->ptr,sdslen(key->ptr),1,fp) == 0) goto werr;
            if (type == REDIS_STRING) {
                /* Save a string value */
                sds sval = o->ptr;
                len = htonl(sdslen(sval));
                if (fwrite(&len,4,1,fp) == 0) goto werr;
                if (sdslen(sval) && fwrite(sval,sdslen(sval),1,fp) == 0) goto werr;
            } else if (type == REDIS_LIST) {
                /* Save a list value */
                list *list = o->ptr;
                listNode *ln = list->head;
                len = htonl(listLength(list));
                if (fwrite(&len,4,1,fp) == 0) goto werr;
                while (ln) {
                    robj *eleobj = listNodeValue(ln);
                    len = htonl(sdslen(eleobj->ptr));
                    if (fwrite(&len,4,1,fp) == 0) goto werr;
                    if (sdslen(eleobj->ptr) && fwrite(eleobj->ptr,sdslen(eleobj->ptr),1,fp) == 0)
                        goto werr;
                    ln = ln->next;
                }
            } else if (type == REDIS_SET) {
                /* Save a set value */
                dict *set = o->ptr;
                dictIterator *di = dictGetIterator(set);
                dictEntry *de;

                if (!set) oom("dictGetIterator");
                len = htonl(dictGetHashTableUsed(set));
                if (fwrite(&len,4,1,fp) == 0) goto werr;
                while ((de = dictNext(di)) != NULL) {
                    robj *eleobj;
                    eleobj = dictGetEntryKey(de);
                    len = htonl(sdslen(eleobj->ptr));
                    if (fwrite(&len,4,1,fp) == 0) goto werr;
                    if (sdslen(eleobj->ptr) && fwrite(eleobj->ptr,sdslen(eleobj->ptr),1,fp) == 0)
                        goto werr;
                }
                dictReleaseIterator(di);
            } else {
                assert(0 != 0);
            }
        }
        dictReleaseIterator(di);
    }
    /* EOF opcode */
    type = REDIS_EOF;
    if (fwrite(&type,1,1,fp) == 0) goto werr;
    fflush(fp);
    fclose(fp);

    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. */
    if (rename(tmpfile,filename) == -1) {
        redisLog(REDIS_WARNING,"Error moving temp DB file on the final destionation: %s",
            strerror(errno));
        unlink(tmpfile);
        return REDIS_ERR;
    }

    redisLog(REDIS_NOTICE,"DB saved on disk");
    server.dirty = 0;
    server.lastsave = time(NULL);
    return REDIS_OK;

werr:
    fclose(fp);
    unlink(tmpfile);
    redisLog(REDIS_WARNING,"Write error saving DB on disk: %s", strerror(errno));
    if (di) dictReleaseIterator(di);
    return REDIS_ERR;
}

static int saveDbBackground(char *filename) {
    pid_t childpid;

    if (server.bgsaveinprogress) return REDIS_ERR;
    if ((childpid = fork()) == 0) {
        /* Child */
        close(server.fd);
        if (saveDb(filename) == REDIS_OK) {
            exit(0);
        } else {
            exit(1);
        }
    } else {
        /* Parent */
        redisLog(REDIS_NOTICE, "Background saving started by pid %d", childpid);
        server.bgsaveinprogress = 1;
        return REDIS_OK;
    }
    return REDIS_OK; // unreached 
}

static int loadDb(char *filename) {
    FILE *fp;
    char buf[REDIS_LOADBUF_LEN];
    char vbuf[REDIS_LOADBUF_LEN];
    char *key = NULL, *val = NULL;
    uint32_t klen, vlen, dbid;
    uint8_t type;
    int retval;
    dict *d = server.dict[0];

    fp = fopen(filename, "r");
    if (!fp) return REDIS_ERR;
    if (fread(buf,9,1,fp) == 0) goto eoferr;
    if (memcmp(buf,"REDIS0000",9) != 0) {
        fclose(fp);
        redisLog(REDIS_WARNING, "Wrong signature trying to load DB from file");
        return REDIS_ERR;
    }

    while (1)
    {
        robj *o;

        /* Read type. */
        if (fread(&type, 1, 1, fp) == 0) goto eoferr;
        if (type == REDIS_EOF) break;
        /* Handle SELECT DB opcode as a special case */
        if (type == REDIS_SELECTDB) {
            if (fread(&dbid,4,1,fp) == 0) goto eoferr;
            dbid = ntohl(dbid);
            if (dbid >= (unsigned)server.dbnum) {
                redisLog(REDIS_WARNING, "FATAL: Data file was created with a Redis server compiled \
                    to handle more than %d databases. Exiting\n", server.dbnum);
                exit(1);
            }
            d = server.dict[dbid];
            continue;
        }
        /* Read key */
        if (fread(&klen,4,1,fp) == 0) goto eoferr;
        klen = ntohl(klen);
        if (klen <= REDIS_LOADBUF_LEN) {
            key = buf;
        } else {
            key = zmalloc(klen);
            if (!key) oom("Loading DB from file");
        }
        if (fread(key,klen,1,fp) == 0) goto eoferr;

        if (type == REDIS_STRING) {
            /* Read string value */
            if (fread(&vlen,4,1,fp) == 0) goto eoferr;
            vlen = ntohl(vlen);
            if (vlen <= REDIS_LOADBUF_LEN) {
                val = vbuf;
            } else {
                val = zmalloc(vlen);
                if (!val) oom("Loading DB from file");
            }
            if (vlen && fread(val,vlen,1,fp) == 0) goto eoferr;
            o = createObject(REDIS_STRING,sdsnewlen(val,vlen));
        } else if (type == REDIS_LIST || type == REDIS_SET) {
            /* Read list/set value */
            uint32_t listlen;
            if (fread(&listlen,4,1,fp) == 0) goto eoferr;
            listlen = ntohl(listlen);
            o = (type == REDIS_LIST) ? createListObject() : createSetObject();
            /* Load every single element of the list/set */
            while (listlen--) {
                robj *ele;
                if (fread(&vlen,4,1,fp) == 0) goto eoferr;
                vlen = ntohl(vlen);
                if (vlen <= REDIS_LOADBUF_LEN) {
                    val = vbuf;
                } else {
                    val = zmalloc(vlen);
                    if (!val) oom("Loading DB from file");
                }
                if (vlen && fread(val,vlen,1,fp) == 0) goto eoferr;
                ele = createObject(REDIS_STRING,sdsnewlen(val,vlen));
                if (type == REDIS_LIST) {
                    if (!listAddNodeTail((list*)o->ptr,ele))
                        oom("listAddNodeTail");
                } else {
                    if (dictAdd((dict*)o->ptr, ele, NULL) == DICT_ERR)
                        oom("dictAdd");
                }
                /* free the tmp buffer if needed */
                if (val != vbuf) zfree(val);
                val = NULL;
            }
        } else {
            assert(0 != 0);
        }
        /* Add the new object in the hash table */
        retval = dictAdd(d,createStringObject(key,klen),o);
        if (retval == DICT_ERR) {
            redisLog(REDIS_WARNING, "Loading DB, duplicated key found! Unrecoverable error, exiting now.");
            exit(1);
        }
        /* Iteration cleanup */
        if (key != buf) zfree(key);
        if (val != vbuf) zfree(val);
        key = val = NULL;
    }
    fclose(fp);
    return REDIS_OK;

eoferr: /* unexpected end of file is handled here with a fatal exit */
    if (key != buf) zfree(key);
    if (val != vbuf) zfree(val);
    redisLog(REDIS_WARNING,"Short read loading DB. Unrecoverable error, exiting now.");
    exit(1);
    return REDIS_ERR;
}

/* ===================================== Replication ====================================*/

static int syncWrite(int fd, void *ptr, ssize_t size, int timeout) {
    ssize_t nwritten, ret = size;
    time_t start = time(NULL);

    timeout++;
    while (size) {
        if (aeWait(fd,AE_WRITABLE,1000) & AE_WRITABLE) {
            nwritten = write(fd,ptr,size);
            if (nwritten == -1) return -1;
            ptr += nwritten;
            size -= nwritten;
        }
        if ((time(NULL)-start) > timeout) {
            errno = ETIMEDOUT;
            return -1;
        }
    }
    return ret;
}

static int syncRead(int fd, void *ptr, ssize_t size, int timeout)
{
    ssize_t nread, totread = 0;
    time_t start = time(NULL);
    timeout++;

    while(size) {
        if (aeWait(fd,AE_READABLE,1000) & AE_READABLE) {
            nread = read(fd,ptr,size);
            if (nread == -1) return -1;
            ptr += nread;
            size -= nread;
            totread += nread;
        }
        if ((time(NULL)-start) > timeout) {
            errno = ETIMEDOUT;
            return -1;
        }
    }
    return totread;
}

static int syncReadLine(int fd, char *ptr, ssize_t size, int timeout)
{
    ssize_t nread = 0;
    size--;
    while (size) {
        char c;
        if (syncRead(fd,&c,1,timeout) == -1) return -1;
        if (c == '\n') {
            *ptr = '\0';
            if (nread && *(ptr-1) == '\r') *(ptr-1) = '\0';
            return nread;
        } else {
            *ptr++ = c;
            *ptr = '\0';
            nread++;
        }
    }
    return nread;
}

static int syncWithMaster(void) {
    char buf[1024], tmpfile[256];
    int dumpsize;
    int fd = anetTcpConnect(NULL,server.masterhost,server.masterport);
    int dfd;

    if (fd == -1) {
        redisLog(REDIS_WARNING,"Unable to connect to MASTER: %s", strerror(errno));
        return REDIS_ERR;
    }

    /* Issue the SYNC command */
    if (syncWrite(fd,"SYNC \r\n",7,5) == -1) {
        close(fd);
        redisLog(REDIS_WARNING,"I/O error writing to MASTER: %s", strerror(errno));
        return REDIS_ERR;
    }

    /* Read the bulk write count */
    if (syncReadLine(fd,buf,1024,5) == -1) {
        close(fd);
        redisLog(REDIS_WARNING,"I/O error reading bulk count from MASTER: %s",
            strerror(errno));
        return REDIS_ERR;
    }
    dumpsize = atoi(buf);
    redisLog(REDIS_NOTICE, "Receiving %d bytes data dump from MASTER", dumpsize);
    /* Read the bulk write data on a temp file */
    snprintf(tmpfile,256,"temp-%d.%ld.rdb",(int)time(NULL),(long int)random());
    dfd = open(tmpfile,O_CREAT|O_WRONLY,0644);
    if (dfd == -1) {
        close(fd);
        redisLog(REDIS_WARNING,"Opening the temp file needed for MASTER <-> SLAVE synchronization: %s", strerror(errno));
        return REDIS_ERR;
    }
    while (dumpsize) {
        int nread, nwritten;

        nread = read(fd,buf,(dumpsize < 1024)?dumpsize:1024);
        if (nread == -1) {
            redisLog(REDIS_WARNING,"I/O error trying to sync with MASTER: %s", strerror(errno));
            close(fd);
            close(dfd);
            return REDIS_ERR;
        }
        nwritten = write(dfd,buf,nread);
        if (nwritten == -1) {
            redisLog(REDIS_WARNING,"Write error writing to the DB dump file needed for MASTER <-> SLAVE synchronization: %s",
                strerror(errno));
            close(fd);
            close(dfd);
            return REDIS_ERR;
        }
        dumpsize -= nread;
    }
    close(dfd);
    if (rename(tmpfile,server.dbfilename) == -1) {
        redisLog(REDIS_WARNING,"Failed trying to rename the temp DB into dump.rdb in MASTER <-> SLAVE synchronization: %s", 
            strerror(errno));
        unlink(tmpfile);
        close(fd);
        return REDIS_ERR;
    }
    emptyDb();
    if (loadDb(server.dbfilename) != REDIS_OK) {
        redisLog(REDIS_WARNING, "Failed trying to load the MASTER synchronization DB from disk");
        close(fd);
        return REDIS_ERR;
    }

    server.master = createClient(fd);
    server.master->flags |= REDIS_MASTER;
    server.replstate = REDIS_REPL_CONNECTED;
    return REDIS_OK;
}

/* =========================== Commands =======================================*/
static void pingCommand(redisClient *c) {
    addReply(c,shared.pong);
}

static void echoCommand(redisClient *c) {
    addReplySds(c,sdscatprintf(sdsempty(), "%d\r\n", (int)sdslen(c->argv[1]->ptr)));
}

/* ================================ CMD - Strings ==========================*/
static void setGenericCommand(redisClient *c, int nx) {
    int retval;

    retval = dictAdd(c->dict,c->argv[1],c->argv[2]);
    if (retval == DICT_ERR) {
        if (!nx) {
            dictReplace(c->dict,c->argv[1],c->argv[2]);
            incrRefCount(c->argv[2]);
        } else {
            addReply(c,shared.zero);
            return;
        }
    } else {
        incrRefCount(c->argv[1]);
        incrRefCount(c->argv[2]);
    }
    server.dirty++;
    addReply(c, nx ? shared.one : shared.ok);
}

static void setCommand(redisClient *c) {
    return setGenericCommand(c,0);
}

static void setnxCommand(redisClient *c) {
    return setGenericCommand(c,1);
}

static void getCommand(redisClient *c) {
    dictEntry *de;
    de = dictFind(c->dict, c->argv[1]);
    if (de == NULL) {
        addReply(c,shared.nil);
    } else {
        robj *o = dictGetEntryVal(de);
        if (o->type != REDIS_STRING) {
            addReply(c,shared.wrongtypeerrbulk);
        } else {
            addReplySds(c,sdscatprintf(sdsempty(),"%d\r\n",(int)sdslen(o->ptr)));
            addReply(c,o);
            addReply(c,shared.crlf);
        }
    }
}


/* ======================================= Main! ======================================*/

static void daemonize(void) {
    int fd;
    FILE *fp;

    if (fork() != 0) exit(0); /* parent exits */
    setsid();  /* create a new session */

    /* Every output goes to /dev/null. If Redis is daemonized but
     * the 'logfile' is set to 'stdout' in the configuration file
     * it will not log at all. */
    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
    /* Try to write the pid file */
    fp = fopen("/var/run/redis.pid","w");
    if (fp) {
        fprintf(fp,"%d\n",getpid());
        fclose(fp);
    }
}

int main(int argc, char **argv) {
    initServerConfig();
    if (argc == 2) {
        ResetServerSaveParams();
        loadServerConfig(argv[1]);
    } else if (argc > 2) {
        fprintf(stderr, "Usage: ./redis-server [/path/to/reids.conf]\n");
        exit(1);
    }
    initServer();
    if (server.daemonize) daemonize();
    redisLog(REDIS_NOTICE, "Server started, Redis version " REDIS_VERSION);
    if (loadDb(server.dbfilename) == REDIS_OK) {
        redisLog(REDIS_NOTICE, "DB loaded from disk");
    }
    if (aeCreateFileEvent(server.el, server.fd, AE_READABLE, 
            acceptHandler, NULL, NULL) == AE_ERR) oom("creating file event");
    redisLog(REDIS_NOTICE, "The server is now ready to accept connections");
    aeMain(server.el);
    aeDeleteEventLoop(server.el);
    return 0;

}