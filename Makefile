CC = g++
CFLAGS = -pthread -std=c++11 -Wall -Wextra -g

all : main

main : MapReduceFramework.a Search	

MapReduceFramework.a : MapReduceFramework.o 
	ar rcs MapReduceFramework.a MapReduceFramework.o


Search : Search.o MapReduceFramework.o
	${CC} ${CFLAGS} MapReduceFramework.o Search.o -o Search

Search.o : Search.cpp
	${CC} ${CFLAGS} Search.cpp -c

    MapReduceFramework.o : MapReduceFramework.cpp MapReduceFramework.h MapReduceClient.h
	${CC} ${CFLAGS} MapReduceFramework.cpp -c

clean :
	rm -f  Search *.o *.a .MapReduceFramework.log ex3.tar

valgrind : main
	valgrind --leak-check=full --show-possibly-lost=yes --show-reachable=yes --undef-value-errors=yes

tar:
	tar -cvf ex3.tar Makefile README MapReduceFramework.cpp Search.cpp

.PHONY: clean, all, tar
