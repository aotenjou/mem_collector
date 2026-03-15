EXTENSION = mem_collector
MODULE_big = mem_collector
OBJS = mem_collector.o \
	mem_collector_common.o \
	mem_collector_state.o \
	mem_collector_feature.o \
	mem_collector_runtime.o \
	mem_collector_storage.o \
	mem_collector_api.o \
	mem_collector_hooks.o \
	mem_collector_bgw.o
DATA = mem_collector--1.0.sql

PG_CONFIG ?= /usr/lib/postgresql/12/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
