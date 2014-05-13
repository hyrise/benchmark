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
#include <unistd.h>

#include "perf.h"
#include "util.h"
#include "cpu.h"
#include "sampler.h"
#include "util.h"

/* 2^n size of event ring buffer (in pages) */
#define BUF_SIZE_SHIFT 8

pid_t execute(char **argv, int argc)
{
    pid_t pid = 0;
    int i = 0;
    
    printf("Executing command:");
    for (i=0; i<argc; ++i) {
      printf(" %s", argv[i]);
    }
    printf("\n");

    if ((pid = fork()) < 0) {     /* fork a child process           */
        printf("*** ERROR: forking child process failed\n");
        exit(1);
    }
    else if (pid == 0) {          /* for the child process:         */
      printf("child executing\n");
        if (execvp(*argv, argv) < 0) {     /* execute the command  */
             printf("*** ERROR: exec failed\n");
             exit(1);
        }
        printf("child finished\n");
    }

    return pid; 
    }

void wait_for_child_process(pid_t  pid) {
  int status;
  pid_t w;

  do {
    w = waitpid(pid, &status, WUNTRACED | WCONTINUED);
    if (w == -1) {
      perror("waitpid");
      exit(1);
    }
  } while (!WIFEXITED(status) && !WIFSIGNALED(status));
}

void wait_for_other_process(pid_t  pid) {
  unsigned int microseconds = 500;

  /* wait for completion  */
  while (kill(pid, 0) != -1)      
  {
    usleep(microseconds);
  }
}

void write_result_samples(char* filename, struct perf_fd *pfd)
{
  struct perf_iter iter;

  if (filename == NULL) {
    filename = "loads.perf.csv";
  }

  FILE *f = fopen(filename, "w");
  if (f == NULL)
  {
      printf("Error opening result file (%s)!\n", filename);
      exit(1);
  }

  printf("Writing samples to result file %s\n", filename);

  /* print header */
  const char *header1 = "# time; sample_id; cpu; pid; tid;";
  const char *header2 = "ip; data_address; data_latency; data_source;";
  const char *header3 = "memory_operation; memory_level; tlb_access;";
  const char *header4 = "memory_operation_str; memory_level_str; tlb_access_str";
  fprintf(f, "%s %s %s %s\n", header1, header2, header3, header4);

  perf_iter_init(&iter, pfd);
  int samples = 0, others = 0, throttled = 0, skipped = 0, kernel = 0;
  u64 lost = 0;

  while (!perf_iter_finished(&iter)) {
    char buffer[64];
    struct perf_event_header *hdr = perf_buffer_read(&iter, buffer, 64);

    if (!hdr) {
      skipped++;
      continue;
    }

    if (hdr->type != PERF_RECORD_SAMPLE) {
      if (hdr->type == PERF_RECORD_THROTTLE)
        throttled++;
      else if (hdr->type == PERF_RECORD_LOST)
        lost += perf_hdr_payload(hdr)[1];
      else
        others++;
      continue;
    }
    samples++;
    if (hdr->size != 72) {
      printf("unexpected sample size %d\n", hdr->size);
      continue;
    }

    u64 *payload = perf_hdr_payload(hdr);
    size_t i = 0;
    
    u64 ip = payload[i++];
    u32 *pid_tid = (u32*)&payload[i++];
    u32 pid = pid_tid[0];
    u32 tid = pid_tid[1];
    u64 timestamp = payload[i++];
    u64 addr = payload[i++];
    u64 sampleid = payload[i++];
    u64 cpu = payload[i++];
    u64 weight = payload[i++];
    u64 src = payload[i++];
    union perf_mem_data_src *data_source = (union perf_mem_data_src *)&src;

    /* Filter out kernel samples, which can happen due to OOO skid */
    if ((long long)addr < 0) {
      ++kernel;
      // continue;
    }

    fprintf(f, "%llu;", timestamp);
    fprintf(f, "%llu;", sampleid);
    fprintf(f, "%llu;", cpu);
    fprintf(f, "%u;", pid);
    fprintf(f, "%u;", tid);
    fprintf(f, "%llx;", ip);
    fprintf(f, "%llx;", addr);
    fprintf(f, "%llu;", weight);
    fprintf(f, "%llu;", src);
    fprintf(f, "%u;", data_source->mem_op);
    fprintf(f, "%u;", data_source->mem_lvl);
    fprintf(f, "%u;", data_source->mem_dtlb);

    // memory operation
    if (data_source->mem_op & PERF_MEM_OP_NA)                 { fprintf(f, "not-available;"); }
    else if (data_source->mem_op & PERF_MEM_OP_LOAD)          { fprintf(f, "load;"); }
    else if (data_source->mem_op & PERF_MEM_OP_STORE)         { fprintf(f, "store;"); }
    else if (data_source->mem_op & PERF_MEM_OP_PFETCH)        { fprintf(f, "prefetch;"); }
    else if (data_source->mem_op & PERF_MEM_OP_EXEC)          { fprintf(f, "exec;"); }

    // memory level
    if (data_source->mem_lvl & PERF_MEM_LVL_NA)               { fprintf(f, "NA"); }
    else {
      if (data_source->mem_lvl & PERF_MEM_LVL_L1)             { fprintf(f, "L1"); }
      else if (data_source->mem_lvl & PERF_MEM_LVL_L2)        { fprintf(f, "L2"); }
      else if (data_source->mem_lvl & PERF_MEM_LVL_L3)        { fprintf(f, "L3"); }
      else if (data_source->mem_lvl & PERF_MEM_LVL_LFB)       { fprintf(f, "LFB"); }
      else if (data_source->mem_lvl & PERF_MEM_LVL_LOC_RAM)   { fprintf(f, "MEM0"); }
      else if (data_source->mem_lvl & PERF_MEM_LVL_REM_RAM1)  { fprintf(f, "MEM1"); }
      else if (data_source->mem_lvl & PERF_MEM_LVL_REM_RAM2)  { fprintf(f, "MEM2"); }
      else if (data_source->mem_lvl & PERF_MEM_LVL_REM_CCE1)  { fprintf(f, "remote-cache-1"); }
      else if (data_source->mem_lvl & PERF_MEM_LVL_REM_CCE2)  { fprintf(f, "remote-cache-2"); }
      else if (data_source->mem_lvl & PERF_MEM_LVL_IO)        { fprintf(f, "IO"); }
      else if (data_source->mem_lvl & PERF_MEM_LVL_UNC)       { fprintf(f, "uncached"); }
      
      if (data_source->mem_lvl & PERF_MEM_LVL_HIT)            { fprintf(f, "-hit"); }
      else if (data_source->mem_lvl & PERF_MEM_LVL_MISS)      { fprintf(f, "-miss"); }
    }
    fprintf(f, ";");

    // tlb access
    if (data_source->mem_dtlb & PERF_MEM_TLB_NA)              { fprintf(f, "NA"); }
    else {
      if (data_source->mem_dtlb & PERF_MEM_TLB_L1)            { fprintf(f, "L1"); }
      else if (data_source->mem_dtlb & PERF_MEM_TLB_L2)       { fprintf(f, "L2"); }
      else if (data_source->mem_dtlb & PERF_MEM_TLB_WK)       { fprintf(f, "hardware-walk"); }
      else if (data_source->mem_dtlb & PERF_MEM_TLB_OS)       { fprintf(f, "os-handler"); }
      
      if (data_source->mem_dtlb & PERF_MEM_TLB_HIT)           { fprintf(f, "-hit"); }
      else if (data_source->mem_dtlb & PERF_MEM_TLB_MISS)     { fprintf(f, "-miss"); }
    }
    fprintf(f, "\n");
  }

  perf_iter_continue(&iter);

  printf("%d samples, %d others, %llu lost, %d throttled, %d skipped, %d kernel\n",
        samples,
        others,
        lost,
        throttled,
        skipped,
        kernel);
  
  fclose(f);
}

void write_result_nosamples(char* filename, long long cycles, long long cycles_ref, long long inst_retired, long long llc_ref, long long llc_miss)
{
  FILE *f = fopen(filename, "w");
  if (f == NULL)
  {
      printf("Error opening result file (%s)!\n", filename);
      exit(1);
  }

  printf("Writing to result file %s\n", filename);

  /* print header */
  const char *header1 = "# cycles; cycles_ref; inst_retired; llc_ref; llc_miss";
  fprintf(f, "%s\n", header1);

  /* print values */
  fprintf(f, "%lld;%lld;%lld;%lld;%lld\n", cycles, cycles_ref, inst_retired, llc_ref, llc_miss);

  /* close file */
  fclose(f);
}

void open_perf_counter(struct perf_fd *loads, pid_t pid, u64 perf_counter) {
  /* Set up perf for loads */
  struct perf_event_attr attr;
  memset(&attr, 0, sizeof(struct perf_event_attr));
  attr.type = PERF_TYPE_RAW;
  attr.size = sizeof(struct perf_event_attr);
  attr.config = perf_counter;  /* Event */
  attr.disabled = 1;
  attr.exclude_kernel = 1;
  attr.exclude_hv = 1;

  if (perf_fd_open(loads, &attr, BUF_SIZE_SHIFT, pid) < 0)
    err("error: perf event init loads");
  printf("open_perf_counter event %llx\n", attr.config);
}

void open_perf_counter_load_sampling(struct perf_fd *loads, pid_t pid) {
  /* Set up perf for loads */
  struct perf_event_attr attr;
  memset(&attr, 0, sizeof(struct perf_event_attr));
  attr.type = PERF_TYPE_RAW;
  attr.size = PERF_ATTR_SIZE_VER0;
  attr.sample_type = PERF_SAMPLE_IP | PERF_SAMPLE_TID | PERF_SAMPLE_TIME | PERF_SAMPLE_ADDR | PERF_SAMPLE_ID| PERF_SAMPLE_CPU | PERF_SAMPLE_WEIGHT | PERF_SAMPLE_DATA_SRC;
  attr.sample_period = 100000;   /* Period */
  attr.exclude_kernel = 0;
  attr.precise_ip = 1;    /* Enable PEBS */
  attr.config1 = 3;     /* Load Latency threshold */
  attr.config = mem_loads_event();  /* Event */
  attr.disabled = 1;
  attr.sample_id_all = 1;

  if (attr.config == (__u64)-1) {
    printf("Unknown CPU model\n");
    exit(1);
  }

  if (perf_fd_open(loads, &attr, BUF_SIZE_SHIFT, pid) < 0)
    err("perf event init loads");
  printf("open_perf_counter event %llx (sampling)\n", attr.config);
}

void start_perf_counter(struct perf_fd *loads) {
  if (perf_enable(loads) < 0)
    err("PERF_EVENT_IOC_ENABLE");
}

void stop_perf_counter(struct perf_fd *loads) {
  if (perf_disable(loads) < 0)
    err("PERF_EVENT_IOC_DISABLE");
}

void close_perf_counter(struct perf_fd *loads) {
  perf_fd_close(loads);
}



