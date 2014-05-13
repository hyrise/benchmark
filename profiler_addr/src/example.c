/* 
 * perf address sampling self profiling demo.
 * Requires a 3.10+ kernel with PERF_SAMPLE_ADDR support and a supported Intel CPU.
 *
 * Copyright (c) 2013 Intel Corporation
 * Author: Andi Kleen
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that: (1) source code distributions
 * retain the above copyright notice and this paragraph in its entirety, (2)
 * distributions including binary code include the above copyright notice and
 * this paragraph in its entirety in the documentation or other materials
 * provided with the distribution
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND WITHOUT ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
 */

#include <linux/perf_event.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <cpuid.h>
#include <stdbool.h>
#include <assert.h>
#include <dlfcn.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
// #include <unistd.h>

#include "perf.h"
#include "util.h"
#include "cpu.h"
#include "sampler.h"
#include "util.h"

#include <papi.h>


void print_usage() {
  printf("Usage: example -s SIZE [-o OUTPUT_DIR]\n");
  printf("\t-s Array size. Specifies the number of char pointers in array.\n");
  printf("\t-o Output directory for results file. Default is CWD.\n");
}

void _mkdir(char* dir) {
  struct stat sb;
  if (stat(dir, &sb) == 0 && S_ISDIR(sb.st_mode)) {
    return;
  }

  char tmp[512];
  char* p = NULL;
  size_t len;

  snprintf(tmp, sizeof(tmp), "%s", dir);
  len = strlen(tmp);
  if (tmp[len - 1] == '/')
    tmp[len - 1] = 0;
  for (p = tmp + 1; *p; p++)
    if (*p == '/') {
      *p = 0;
      mkdir(tmp, S_IRWXU);
      *p = '/';
    }
  mkdir(tmp, S_IRWXU);

  if (stat(dir, &sb) == 0 && S_ISDIR(sb.st_mode)) {
    return;
  } else {
    printf("Could not create directory: %s\n", dir);
    exit(0);
  }
}

void clearCache() {
    int sum = 0;
    int cachesize_in_mb = 60;
    int * dummy_array = malloc(sizeof(int)*1024*1024*cachesize_in_mb);
    int address = 0;
    int repetition = 0;

    printf("clearing the cache...\n");
    for ( address = 0; address < 1024*1024*cachesize_in_mb; address++) {
      dummy_array[address] = address +1;
    }

    int * dummy_array2 = malloc(sizeof(int)*1024*1024*cachesize_in_mb);
    for ( address = 0; address < 1024*1024*cachesize_in_mb; address++) {
      dummy_array2[address] = address +1;
    }

    for(repetition = 0; repetition < 3; repetition++) {
      for ( address = 0; address < 1024*1024*cachesize_in_mb; address++) {
        sum += dummy_array[address];
      } 
    }

    printf("clearing sum is: %d\n", sum);
    free(dummy_array);
    free(dummy_array2);    
}


void init_array(char** array, int array_size) {
  
  printf("initializing array of size %d\n", array_size);

  int i = 0; int r = 0; int remaining = 0; int tmp = 0;
  srand(time(NULL));

  // array of all entries, except entry 0
  int* v = malloc(sizeof(int) * array_size);
  for(i=0; i<array_size; ++i) {
    v[i] = i;
  }

  // create random pointer chasing array  
  for(i=0; i<array_size; ++i)
  {
    // get random value from remaining values  
    remaining = array_size - i;

    do {
      r = rand() % remaining;
    } while (remaining > 2 && (v[r] == i || v[r] == 0)); // no circles, except its the end

    // set value in array to chosen one
    array[i] = (char*)(array + v[r]);

    // swap values so we do not use this one again and create a circle
    tmp = v[r];
    v[r] = v[remaining-1];
    v[remaining-1] = tmp;
  }

  free(v);
}

void execute_mem_access(char** array, int array_size, int num_accesses, int loops) {
  printf("array start is %p\n", array);
  printf("array end is %p\n", array + array_size);
  printf("executing %d memory accesses times %d...\n", num_accesses, loops);

  int i = 0; int k = 0;
  char **p = array;

  for (k=0; k<loops; ++k) {
    for (i=0; i<num_accesses; ++i) {
      p = (char**)*p;
    }
  }

  printf("final address is: %p\n", p);
}

int execute_program(int array_size, char* output_folder) {

  const int num_accesses = 1024*1024*32;
  const int loops = 64;

  char out[128];
  _mkdir(output_folder);

  char** array = malloc(sizeof(char*) * array_size);
  struct perf_fd fd_loads;
  struct perf_fd fd_cycles;
  struct perf_fd fd_cycles_ref;
  struct perf_fd fd_inst_retired;
  struct perf_fd fd_llc_ref;
  struct perf_fd fd_llc_miss;
 
  long long cycles = 0;
  long long cycles_ref = 0;
  long long inst_retired = 0;
  long long llc_ref = 0;
  long long llc_miss = 0;

  open_perf_counter_load_sampling(&fd_loads, 0);
  open_perf_counter(&fd_cycles, 0, ARCH_CYCLE);
  open_perf_counter(&fd_cycles_ref, 0, ARCH_CYCLE_REF);
  open_perf_counter(&fd_inst_retired, 0, ARCH_INST_RETIRED);
  open_perf_counter(&fd_llc_ref, 0, ARCH_LLC_REF);
  open_perf_counter(&fd_llc_miss, 0, ARCH_LLC_MISS);

  init_array(array, array_size);
  clearCache();

  start_perf_counter(&fd_loads);
  start_perf_counter(&fd_cycles);
  start_perf_counter(&fd_cycles_ref);
  start_perf_counter(&fd_inst_retired);
  start_perf_counter(&fd_llc_ref);
  start_perf_counter(&fd_llc_miss);

  time_t start_t = time(NULL);
  execute_mem_access(array, array_size, num_accesses, loops);
  double elapsed_time = (double)(time(NULL) - start_t);
  
  stop_perf_counter(&fd_loads);
  stop_perf_counter(&fd_cycles);
  stop_perf_counter(&fd_cycles_ref);
  stop_perf_counter(&fd_inst_retired);
  stop_perf_counter(&fd_llc_ref);
  stop_perf_counter(&fd_llc_miss);

  printf("Elaspsed time: %.2f seconds\n", elapsed_time);
  
  cycles = perf_read_value(&fd_cycles);
  cycles_ref = perf_read_value(&fd_cycles_ref);
  inst_retired = perf_read_value(&fd_inst_retired);
  llc_ref = perf_read_value(&fd_llc_ref);
  llc_miss = perf_read_value(&fd_llc_miss);

  printf("Cycles: %lld\n", cycles);
  printf("Cycles (ref): %lld\n", cycles_ref);
  printf("Instructions Retired: %lld\n", inst_retired);
  printf("LLC References: %lld\n", llc_ref);
  printf("LLC Misses: %lld\n", llc_miss);

  snprintf(out, 128,"%s/size_%d_loads.perf.csv",output_folder, array_size);
  write_result_samples(out, &fd_loads);

  snprintf(out, 128,"%s/size_%d_stats.perf.csv",output_folder, array_size);
  write_result_nosamples(out, cycles, cycles_ref, inst_retired, llc_ref, llc_miss);
  
  close_perf_counter(&fd_loads);
  close_perf_counter(&fd_cycles);
  close_perf_counter(&fd_cycles_ref);
  close_perf_counter(&fd_inst_retired);
  close_perf_counter(&fd_llc_ref);
  close_perf_counter(&fd_llc_miss);

  free(array);
  return 0;
}

int main(int argc, char **argv)
{
  int array_size = 0;
  char* output_folder = NULL;

  // skip program name
  argv++;
  argc--;

  while (argc > 0) {
    
    const char *cmd = argv[0];

    if (!strcmp(cmd, "--help")) {
      print_usage();
      exit(0);
      break;
    }
    else if (!strcmp(cmd, "-s")) {
      if (argc < 2) {
        print_usage();
        exit(0);
      } else {
        array_size = atoi(argv[1]);
        argv++; argv++;
        argc--; argc--;
      }
    }
    else if (!strcmp(cmd, "-o")) {
      if (argc < 2) {
        print_usage();
        exit(0);
      } else {
        output_folder = argv[1];
        argv++; argv++;
        argc--; argc--;
      }
    } else {
      printf("Unknown parameter\n");
      print_usage();
      exit(0);
    }
  }

  if (array_size == 0) {
    printf("No array size specified\n");
    print_usage();
    exit(0);
  }

  return execute_program(array_size, output_folder);
}


