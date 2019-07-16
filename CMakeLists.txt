cmake_minimum_required(VERSION 3.5)
project(destor C)

set(CMAKE_C_STANDARD 11)

include_directories(src)
include_directories(src/chunking)
include_directories(src/fsl)
include_directories(src/index)
include_directories(src/recipe)
include_directories(src/storage)
include_directories(src/utils)
#include_directories(/usr/lib/x86_64-linux-gnu/glib-2.0/include/)
#include_directories(/usr/include/glib-2.0/)
#include_directories(/root/anaconda3/include/)
include_directories(/usr/local/include/glib-2.0/)
include_directories(/usr/local/lib/glib-2.0/include/)
include_directories(/home/zxy/destor/isaInclude)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")

add_executable(destor
        src/chunking/ae_chunking.c
        src/chunking/chunking.h
        src/chunking/rabin_chunking.c
        src/chunking/fascdc_chunking.c
        src/chunking/md5.h
        src/chunking/md5.c
        src/fsl/libhashfile.c
        src/fsl/libhashfile.h
        src/fsl/read_fsl_trace.c
        src/index/fingerprint_cache.c
        src/index/fingerprint_cache.h
        src/index/index.c
        src/index/index.h
        src/index/index_buffer.h
        src/index/kvstore.c
        src/index/kvstore.h
        src/index/kvstore_htable.c
        src/index/sampling_method.c
        src/index/segmenting_method.c
        src/index/similarity_detection.c
        src/recipe/recipestore.c
        src/recipe/recipestore.h
        src/storage/containerstore.c
        src/storage/containerstore.h
        src/utils/bloom_filter.c
        src/utils/bloom_filter.h
        src/utils/lru_cache.c
        src/utils/lru_cache.h
        src/utils/queue.c
        src/utils/queue.h
        src/utils/sds.c
        src/utils/sds.h
        src/utils/serial.c
        src/utils/serial.h
        src/utils/sync_queue.c
        src/utils/sync_queue.h
        src/assembly_restore.c
        src/backup.h
        src/cap_rewrite.c
        src/cbr_rewrite.c
        src/cfl_rewrite.c
        src/chunk_phase.c
        src/cma.c
        src/cma.h
        src/config.c
        src/dedup_phase.c
        src/destor.c
        src/destor.h
        src/do_backup.c
        src/do_delete.c
        src/do_restore.c
        src/filter_phase.c
        src/har_rewrite.c
        src/hash_phase.c
        src/jcr.c
        src/jcr.h
        src/optimal_restore.c
        src/read_phase.c
        src/restore.h
        src/restore_aware.c
        src/rewrite_phase.c
        src/rewrite_phase.h
        src/trace_phase.c)

target_link_libraries(destor glib-2.0 pthread ssl crypto isal_crypto)
