
#define CPUFREQ 2000LLU /* MHz */
#define NS2CYCLE(__ns) ((__ns) * CPUFREQ / 1000)
#define LOG_MODE (0)

#ifdef __i386__
#include <emmintrin.h>
#else
#ifdef __amd64__
#include <emmintrin.h>
#endif
#endif

#define CACHELINE_SIZE (64)

#define _mm_clflush(addr)\
	asm volatile("clflush %0" : "+m" (*(volatile char *)(addr)))
#define _mm_clflushopt(addr)\
	asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(addr)))
#define _mm_clwb(addr)\
	asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(addr)))
#define _mm_pcommit()\
	asm volatile(".byte 0x66, 0x0f, 0xae, 0xf8")

extern int extra_latency;

typedef uint64_t pcm_hrtime_t;
#if defined(__i386__)


static inline unsigned long long asm_rdtsc(void)
{
    unsigned long long int x;
    __asm__ volatile (".byte 0x0f, 0x31" : "=A" (x));
    return x;
}

static inline unsigned long long asm_rdtscp(void)
{
        unsigned hi, lo;
    __asm__ __volatile__ ("rdtscp" : "=a"(lo), "=d"(hi)::"ecx");
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );

}
#elif defined(__x86_64__)

static inline unsigned long long asm_rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

static inline unsigned long long asm_rdtscp(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtscp" : "=a"(lo), "=d"(hi)::"rcx");
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}
#else
#error "What architecture is this???"
#endif

# ifdef _EMULATE_LATENCY_USING_NOPS
static inline void asm_nop10() {
	asm volatile("nop");
	asm volatile("nop");
	asm volatile("nop");
	asm volatile("nop");
	asm volatile("nop");
	asm volatile("nop");
	asm volatile("nop");
	asm volatile("nop");
	asm volatile("nop");
	asm volatile("nop");
}

static inline
void
emulate_latency_ns(extra_latency)
{
	int          i;
	pcm_hrtime_t cycles;
	pcm_hrtime_t start;
	pcm_hrtime_t stop;
	
	cycles = NS2CYCLE(extra_latency);
	for (i=0; i<cycles; i+=5) {
		asm_nop10();
	}
}

# else

static inline
void
emulate_latency_ns(extra_latency)
{
	pcm_hrtime_t cycles;
	pcm_hrtime_t start;
	pcm_hrtime_t stop;
	
	start = asm_rdtsc();
	cycles = NS2CYCLE(extra_latency);

	do { 
		/* RDTSC doesn't necessarily wait for previous instructions to complete 
		 * so a serializing instruction is usually used to ensure previous 
		 * instructions have completed. However, in our case this is a desirable
		 * property since we want to overlap the latency we emulate with the
		 * actual latency of the emulated instruction. 
		 */
		stop = asm_rdtsc();
	} while (stop - start < cycles);
}

# endif


static inline void PERSISTENT_BARRIER(void)
{
    asm volatile ("sfence\n" : : );
}

static inline void persistent(void *buf, uint32_t len, int fence)
{
    uint32_t i;
    len = len + ((unsigned long)(buf) & (CACHELINE_SIZE - 1));
    if(fence == 2)
    {
        PERSISTENT_BARRIER();
    }

    

    //if (arch_has_clwb())
    //support_clwb = 1;
    int support_clwb = 0;

    if (support_clwb) {
        for (i = 0; i < len; i += CACHELINE_SIZE)
                _mm_clwb(buf + i);
    } else {
        for (i = 0; i < len; i += CACHELINE_SIZE)
		{
			//pflush(buf+i);
			//emulate_latency_ns(extra_latency);
			extra_latency++;
            _mm_clflush(buf + i);
		}

	//pflush(buf+i);
    }
    /* Do a fence only if asked. We often don't need to do a fence
     * immediately after clflush because even if we get context switched
     * between clflush and subsequent fence, the context switch operation
     * provides implicit fence. */
    if (fence)
        PERSISTENT_BARRIER();
}



