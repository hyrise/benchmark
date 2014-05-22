

/* Fiexd Architecture Events */
#define ARCH_CYCLE                0x013c      /* UnHalted Core Cycles */
#define ARCH_CYCLE_REF            0x003c      /* UnHalted Reference Cycles */
#define ARCH_INST_RETIRED         0x00c0      /* Instruction Retired */
#define ARCH_LLC_REF              0x4f2e      /* Instruction Retired */
#define ARCH_LLC_MISS             0x412e      /* Instruction Retired */
#define ARCH_BRANCH_INST_RETIRED  0x00c4      /* Instruction Retired */
#define ARCH_BRANCH_MISS_RETIRED  0x00c5      /* Instruction Retired */

/* Sandy Bridge */
#define MEM_LOADS_SNB 0x1cd /* MEM_TRANS_RETIRED.LOAD_LATENCY */
#define MEM_STORES_SNB 0x2cd  /* MEM_TRANS_RETIRED.PRECISE_STORES */

/* Haswell */
#define MEM_LOADS_HSW MEM_LOADS_SNB
#define MEM_STORES_HSW 0x82d0

/* Nehalem and Westmere */
#define MEM_LOADS_NHM 0x100b  /* MEM_INST_RETIRED.LOAD_LATENCY */
#define MEM_STORES_NHM 0  /* not supported */


unsigned mem_loads_event(void);
unsigned mem_stores_event(void);
