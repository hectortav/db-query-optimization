CC = g++
CFLAGS  = -O3 #-g -pg
DEPS=list.h functions.h

default: final

final:  main.o functions.o list.o 
	$(CC) $(CFLAGS) -o final main.o functions.o list.o

main.o:  main.cpp list.h functions.h 
	$(CC) $(CFLAGS) -c main.cpp

functions.o:  functions.cpp functions.h 
	$(CC) $(CFLAGS) -lm -c functions.cpp

list.o:  list.cpp list.h 
	$(CC) $(CFLAGS) -c list.cpp

clean: 
	$(RM) final *.o