
apr-skeleton: apr-skeleton.c 
	gcc apr-skeleton.c -o apr-skeleton -I/usr/local/apr/include/ -L/usr/local/apr/lib -lapr-1 `pkg-config apr-1 --cflags` 

ab: ab.c ap_release.h
	gcc ab.c ap_release.h -O0 -g -o ab -I/usr/local/apr/include/ -L/usr/local/apr/lib -lapr-1 `pkg-config apr-1 --cflags` -laprutil-1 -lm -lgsl -lgslcblas



clean:
	rm ab apr-skeleton

