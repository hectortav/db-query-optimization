CC=g++
CFLAGS=-I -g
DEPS=list.h
OBJ=main.o list.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

final: $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)

.Phony: clean

clean:
	-rm $(OBJ)
	-rm final
