CC=g++
CFLAGS=-I -lm -Wall -g -pg
DEPS=list.h functions.h
OBJ=main.o functions.o list.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS) 

final: $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS) 

.Phony: clean

clean:
	-rm $(OBJ)
	-rm final
