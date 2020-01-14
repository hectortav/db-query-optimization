CC = g++
CFLAGS  = --std=c++11 -g #-pg
DEPS=list.h JobScheduler.h functions.h besttreemap.h

default: final

final:  main.o functions.o list.o JobScheduler.o besttreemap.o
	$(CC) $(CFLAGS) -o final main.o functions.o list.o JobScheduler.o besttreemap.o -pthread

static:	main_static.o functions.o list.o JobScheduler.o
	$(CC) $(CFLAGS) -o final main_static.o functions.o list.o JobScheduler.o -pthread

main.o:  main.cpp list.h JobScheduler.h functions.h 
	$(CC) $(CFLAGS) -c main.cpp

functions.o:  functions.cpp functions.h 
	$(CC) $(CFLAGS) -lm -c functions.cpp

besttreemap.o:  besttreemap.cpp besttreemap.h 
	$(CC) $(CFLAGS) -c besttreemap.cpp

list.o:  list.cpp list.h 
	$(CC) $(CFLAGS) -c list.cpp

JobScheduler.o:  JobScheduler.cpp JobScheduler.h 
	$(CC) $(CFLAGS) -c JobScheduler.cpp

clean: 
	$(RM) final *.o

finalo: maino.o functionso.o listo.o JobSchedulero.o besttreemapo.o
	$(CC) $(CFLAGS) -O3 -o final main.o functions.o list.o JobScheduler.o besttreemap.o -pthread

maino.o:  main.cpp list.h JobScheduler.h functions.h 
	$(CC) $(CFLAGS) -O3 -c main.cpp

functionso.o:  functions.cpp functions.h 
	$(CC) $(CFLAGS) -O3 -lm -c functions.cpp

JobSchedulero.o:  JobScheduler.cpp JobScheduler.h 
	$(CC) $(CFLAGS) -O3 -c JobScheduler.cpp

listo.o:  list.cpp list.h 
	$(CC) $(CFLAGS) -O3 -c list.cpp

besttreemapo.o:  besttreemap.cpp besttreemap.h 
	$(CC) $(CFLAGS) -O3 -c besttreemap.cpp
