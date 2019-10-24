CC=g++
CFLAGS=-I -g -lm
DEPS=list.h functions.h
OBJ=main.o functions.o list.o

%.o: %.c $(DEPS)
	$(CC) -g -c -o $@ $< $(CFLAGS)

final: $(OBJ)
	$(CC) -g -o $@ $^ $(CFLAGS)

.Phony: clean

clean:
	-rm $(OBJ)
	-rm final
