CC      = gcc
CFLAGS  = -O2 -Wall -Wextra -g \
          -I/usr/include/postgresql
LDFLAGS = -L/usr/lib/x86_64-linux-gnu -lpq -lpthread

SRC = src/tpcc.c
BIN = tpcc

.PHONY: all clean

all: $(BIN)

$(BIN): $(SRC)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(BIN)
