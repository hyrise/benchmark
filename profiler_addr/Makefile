
all: latency_profiler example

latency_profiler:
	gcc -g -Wall -O3 ./src/cpu.c ./src/perf.c ./src/sampler.c ./src/latency_profiler.c -o ./bin/latency_profiler

example:
	gcc -g -Wall -O3 ./src/cpu.c ./src/perf.c ./src/sampler.c ./src/example.c -o ./bin/example

clean:
	rm -f ./bin/*
