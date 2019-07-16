/* chunking.h
 the main fuction is to chunking the file!
 */

#ifndef CHUNK_H_
#define CHUNK_H_

void windows_reset();
void chunkAlg_init();
int rabin_chunk_data(unsigned char *p, int n);
int normalized_rabin_init(int expect_chunk_size);
int normalized_rabin_chunk_data(unsigned char *p, int n);

void ae_init();
int ae_chunk_data(unsigned char *p, int n);
int ae_chunk_data_v2(unsigned char *p, int n);

int tttd_chunk_data(unsigned char *p, int n);

void fastcdc_init(uint32_t expectCS);
int fastcdc_chunk_data(unsigned char *p, int n);

#endif
