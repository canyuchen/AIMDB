CC = g++
CFLAGS = -Wall -g -O2 -std=c++11

INCLUDE = -I../system/
CFLAGS += $(INCLUDE)

CPPS = $(shell find ./ -name "*.cc")
OBJS = $(shell find ../system/ -name "*.o")

TARGET = debug_catalog debug_datatype debug_hashtable debug_hashindex debug_mymemory debug_schema debug_errorlog debug_rowtable debug_executor

all: ${TARGET}

clean:
	rm -f ${TARGET}
