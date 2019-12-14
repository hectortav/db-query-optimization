CC = g++
CFLAGS  = -g #-pg
DEPS=list.h functions.h

default: final

final:  main.o functions.o list.o 
	$(CC) $(CFLAGS) -o final main.o functions.o list.o

static:	main_static.o functions.o list.o
	$(CC) $(CFLAGS) -o final main_static.o functions.o list.o

main.o:  main.cpp list.h functions.h 
	$(CC) $(CFLAGS) -c main.cpp

functions.o:  functions.cpp functions.h 
	$(CC) $(CFLAGS) -lm -c functions.cpp

list.o:  list.cpp list.h 
	$(CC) $(CFLAGS) -c list.cpp

clean: 
	$(RM) final *.o

finalo: maino.o functionso.o listo.o
	$(CC) $(CFLAGS) -O3 -o final main.o functions.o list.o

maino.o:  main.cpp list.h functions.h 
	$(CC) $(CFLAGS) -O3 -c main.cpp

functionso.o:  functions.cpp functions.h 
	$(CC) $(CFLAGS) -O3 -lm -c functions.cpp

listo.o:  list.cpp list.h 
	$(CC) $(CFLAGS) -O3 -c list.cpp
