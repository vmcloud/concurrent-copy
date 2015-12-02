CPROG = conc-copy
BINDIR = /usr/bin
CFLAGS =
PTHREAD_LIBS = -lpthread
SOURCES = conc-copy.c
OBJECTS = $(SOURCES:.c=.o)

all: build

install: build
	install -m 755 $(CPROG) "$(BINDIR)/"

build: $(OBJECTS)
	$(CC) $(CFLAGS) $< -o $(CPROG) $(PTHREAD_LIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@ 

clean:
	rm -rf *.o
	rm -rf $(CPROG)

.PHONY: all clean build install
