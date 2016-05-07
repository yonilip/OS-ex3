CC = g++
CFLAGS =  -pthread -std=c++11 -Wall -Wextra

all : main

main : Search.o MapReduceFramework.o
	${CC} ${CFLAGS} MapReduceFramework.o Search.cpp -o MyMain

Search.o : Search.cpp
	${CC} ${CFLAGS} Search.cpp -c

MapReduceFramework.o : MapReduceFramework.cpp
	${CC} ${CFLAGS} MapReduceFramework.cpp -c

clean :
	rm -f  MyMain *.o

valgrind : main
	valgrind --leak-check=full --show-possibly-lost=yes --show-reachable=yes --undef-value-errors=yes

tar:
	tar -cvf ex3.tar Makefile README MapReduceFramework.cpp Search.cpp