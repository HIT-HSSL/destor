/*
 * Author: Yucheng Zhang
 * See his INFOCOM paper for more details.
 */

#include "../destor.h"

#define COMP_SIZE 8

#define my_memcmp(x, y) \
     ({ \
        int __ret; \
        uint64_t __a = __builtin_bswap64(*((uint64_t *) x)); \
        uint64_t __b = __builtin_bswap64(*((uint64_t *) y)); \
        if (__a > __b) \
              __ret = 1; \
        else \
              __ret = -1; \
        __ret;\
      })

static int window_size = 0;

/*
 * Calculating the window size
 */
void ae_init(){
	double e = 2.718281828;
	window_size = 12000/(e-1);
}

/*
 * 	n is the size of string p.
 */
int ae_chunk_data(unsigned char *p, int n) {
	/*
	 * curr points to the current position;
	 * max points to the position of max value;
	 * end points to the end of buffer.
	*/
	unsigned char *curr = p+1, *max = p, *end = p+n-8;
	if (n <= window_size + 8)
		return n;
	for (; curr <= end; curr++) {
		int comp_res = my_memcmp(curr, max);
		if (comp_res < 0) {
			max = curr;
			continue;
		}
		if (curr == max + window_size || curr == p + destor.chunk_max_size)
			return curr - p;
	}
	return n;
}

int ae_chunk_data_v2(unsigned char *p, int n)
{
	    unsigned char *p_curr, *max_str = p;
	        int comp_res = 0, i, end, max_pos = 0;
		    if(n < window_size  - COMP_SIZE)
			            return n;
		        end = n - COMP_SIZE;
			    i = 1;
			        p_curr = p + 1;
				    while (i < end){
					    		int end_pos = max_pos + window_size;
									if (end_pos > end)
														end_pos = end;
											uint64_t __cur = *((uint64_t *) p_curr);
													uint64_t __max = *((uint64_t *) max_str);
													       	while(__cur = *((uint64_t *) p_curr), __max =*((uint64_t *) max_str), __cur <= __max && i < end_pos){
															           	p_curr++;
																			   	i++;
																				        }
																__cur = *((uint64_t *) p_curr);
																		__max = *((uint64_t *) max_str);
																				if (__cur > __max) {
																								max_str = p_curr;
																											max_pos = i;
																													}
																						else
																										return end_pos;
																								p_curr++;
																										i++;
																										    }
				        return n;
}
