CC=g++
CFLAGS=-I -g
DEPS=list.h functions.h
OBJ=main.o list.o functions.o

%.o: %.c $(DEPS)
	$(CC) -g -c -o $@ $< $(CFLAGS)

final: $(OBJ)
	$(CC) -g -o $@ $^ $(CFLAGS)

.Phony: clean

clean:
	-rm $(OBJ)
	-rm final
