
CC = gcc
CFLAGS = -I/usr/local/apr/include/ -L/usr/local/apr/lib -lapr-1 `pkg-config apr-1 --cflags`


all: ab apr-skeleton


apr-skeleton: apr-skeleton.c 
	$(CC) apr-skeleton.c -o apr-skeleton $(CFLAGS)


ab: ab.c ap_release.h
	$(CC) ab.c ap_release.h -O0 -g -o ab $(CFLAGS) -laprutil-1 -lm -lgsl -lgslcblas


clean:
	rm -f ab apr-skeleton

