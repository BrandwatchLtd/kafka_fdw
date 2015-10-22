# contrib/kafka_fdw/Makefile

MODULE_big = kafka_fdw

EXTENSION = kafka_fdw
DATA = kafka_fdw--1.0.sql

REGRESS = kafka_fdw

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
SHLIB_LINK += $(filter -lz -lpthread -lrt, $(LIBS))
SHLIB_LINK += -lrdkafka

test : tests/test_kafka_fdw.o

clean :
	rm tests/test_kafka_fdw.o
