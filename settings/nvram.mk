#### Settings file, separated so we don't have to ignore config.mk

PRODUCTION := 1
USE_PROFILER := 0
USE_V8 := 0
COVERAGE_TESTING := 0
VAMPIR_TRACE := 0
PAPI_TRACE := 0
WITH_MYSQL := 1
VERBOSE_BUILD := 1
PERSISTENCY := NVRAM

# [NVRAM_FILESIZE is specified in MB]
NVRAM_MOUNTPOINT := /mnt/pmfs
NVRAM_FILENAME := hyrise_david
NVRAM_FILESIZE := 1024
NVSIMULATOR_FLUSH_NS := 0
NVSIMULATOR_READ_NS := 0
NVSIMULATOR_WRITE_NS := 0


# Following Option can be set optional

# HYRISE_ALLOCATOR  - Use a custom flag like tcmalloc, jemalloc
# HYRISE_ALLOCATOR:= jemalloc

# COMPILER - Use a flag to customize your compiler
COMPILER:= g++48

# If V8 is used you have to set the path where to find the
# snapshot build
# V8_BASE_DIRECTORY=