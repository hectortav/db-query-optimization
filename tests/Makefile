CC=g++
CFLAGS=-I -g -lm -lcunit
OBJ=main.o utils.o list.o functions.o

main: $(OBJ)
	$(CC) -g -o $@ $^ $(CFLAGS)

main.o: main.cpp
	$(CC) -g -c $^

utils.o: utils/utils.cpp
	$(CC) -g -c $^

list.o: ../list.cpp
	$(CC) -g -c $^

functions.o: ../functions.cpp
	$(CC) -g -c $^

.Phony: clean

clean:
	-rm $(OBJ)
	-rm main