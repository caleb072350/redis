# Redis Makefile

DEBUG?= -g
CFLAGS?= -O0 -Wall -W
CCOPT= $(CFLAGS)

OBJ = adlist.o ae.o anet.o dict.o redis.o sds.o zmalloc.o

PRGNAME = redis-server

all: redis-server

# Deps (use make dep to generate this)
adlist.o: adlist.c adlist.h zmalloc.h
ae.o: ae.c ae.h zmalloc.h
anet.o: anet.c anet.h
dict.o: dict.c dict.h
redis.o: redis.c ae.h sds.h anet.h dict.h adlist.h zmalloc.h
sds.o: sds.c sds.h zmalloc.h
zmalloc.o: zmalloc.c zmalloc.h

redis-server: $(OBJ)
	$(CC) $(DEBUG) -o $(PRGNAME) $(CCOPT)  $(OBJ)
	@echo "this is a test msg"


dep:
	$(CC) -MM *.c

.c.o:
	$(CC) $(DEBUG) -c $(CCOPT) $(COMPILE_TIME) $<


test:
	$(CC) $(DEBUG) -o test test.c

clean:
	rm *.o 

