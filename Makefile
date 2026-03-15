EXTENSION = mem_collector
MODULE_big = mem_collector
OBJS = src/mem_collector.o \
	src/core/mem_collector_common.o \
	src/core/mem_collector_state.o \
	src/collector/mem_collector_feature.o \
	src/collector/mem_collector_runtime.o \
	src/storage/mem_collector_storage.o \
	src/api/mem_collector_api.o \
	src/hooks/mem_collector_hooks.o \
	src/bgw/mem_collector_bgw.o
DATA = mem_collector--1.0.sql

PG_CONFIG ?= /usr/lib/postgresql/12/bin/pg_config
PG_CPPFLAGS += -I$(srcdir)/include
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
