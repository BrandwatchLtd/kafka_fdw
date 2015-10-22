MODULE_big = kafka_fdw
OBJS = kafka_fdw.o

EXTENSION = kafka_fdw
DATA = kafka_fdw--1.0.sql

REGRESS = kafka_fdw

SHLIB_LINK += $(filter -lz -lpthread -lrt, $(LIBS))
SHLIB_LINK += -lrdkafka

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
