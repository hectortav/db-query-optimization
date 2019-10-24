CC=g++
CFLAGS=-I -g -lm
DEPS=list.h functions.h
OBJ=main.o list.o functions.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

final: $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)

.Phony: clean

clean:
	-rm $(OBJ)
	-rm final
