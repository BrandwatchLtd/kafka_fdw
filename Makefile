# contrib/kafka_fdw/Makefile

kafka_fdw.o: CFLAGS += -Wno-switch

MODULE_big = kafka_fdw
OBJS = kafka_fdw.o

EXTENSION = kafka_fdw
DATA = kafka_fdw--1.0.sql

REGRESS = kafka_fdw

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

SHLIB_LINK += $(filter -lz -lpthread -lrt, $(LIBS))
SHLIB_LINK += -lrdkafka

TEST_LIBRARY_OBJECTS = tests/tap/basic.h tests/tap/basic.o tests/tap/float.h tests/tap/float.o tests/tap/macros.h

test : build-tests
	runtests -b tests -s tests -l tests/TESTS

build-tests : tests/kafka_fdw.t.o
	$(CC) -I. $(CFLAGS) $(PG_LIBS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o tests/kafka_fdw-t tests/kafka_fdw.t.o $(TEST_LIBRARY_OBJECTS)

tests/kafka_fdw.t.o : $(TEST_LIBRARY_OBJECTS)

clean :
	rm tests/*-t tests/*.o tests/tap/*.o
