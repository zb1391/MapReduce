##Operating Systems
##Map Reduce Using Thread and Processes
##Zac Brown, Pintu Patel, Priya Mundra
##10/6/2013
##
#Compiler
CC=gcc

#compiler options
CFLAGS =  -Wall

#Linker
LN= $(CC)

#Linker Options
LNFLAGS= -g -lm -pthread 

#Source Files
SOURCES = mapreduce.c

#Include Files
INCLUDES = mapreduce.h

#Objects
OBJECTS = mapreduce.o main.o

all: mapred

.c.o:	$*.c
	$(CC) $(CFLAGS) -c $*.c

mapred:$(OBJECTS)
	$(LN) $(LNFLAGS) -o $@ $(OBJECTS) $(LDLIBS) -lrt

clean:
	rm -f *.o
	rm -f *~

mapreduce.o:    mapreduce.c mapreduce.h
main.o:		main.c mapreduce.h

