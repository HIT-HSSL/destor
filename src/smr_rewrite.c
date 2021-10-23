/*
 * Author   : Xiangyu Zou
 * Date     : 10/23/2021
 * Time     : 15:44
 * Project  : destor
 This source code is licensed under the GPLv2
 */

#include "destor.h"
#include "jcr.h"
#include "rewrite_phase.h"
#include "backup.h"
#include "storage/containerstore.h"

static int64_t chunk_num;

static GHashTable *top;

static GHashTable *existing;

int rewrite_buffer_push_smr(struct chunk* c) {
    g_queue_push_tail(rewrite_buffer.chunk_queue, c);

    if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)
        || CHECK_CHUNK(c, CHUNK_SEGMENT_START) || CHECK_CHUNK(c, CHUNK_SEGMENT_END))
        return 0;

    if (c->id != TEMPORARY_ID) {
        assert(CHECK_CHUNK(c, CHUNK_DUPLICATE));

        if(!g_hash_table_lookup(existing, &c->fp)){
            struct containerRecord tmp_record;
            tmp_record.cid = c->id;
            GSequenceIter *iter = g_sequence_lookup(
                    rewrite_buffer.container_record_seq, &tmp_record,
                    g_record_cmp_by_id,
                    NULL);
            if (iter == NULL) {
                struct containerRecord* record = malloc(
                        sizeof(struct containerRecord));
                record->cid = c->id;
                record->size = c->size;
                /* We first assume it is out-of-order */
                record->out_of_order = 1;
                g_sequence_insert_sorted(rewrite_buffer.container_record_seq,
                                         record, g_record_cmp_by_id, NULL);
            } else {
                struct containerRecord* record = g_sequence_get(iter);
                assert(record->cid == c->id);
                record->size += c->size;
            }

            fingerprint* newfp = (fingerprint*)malloc(sizeof(fingerprint));
            memcpy(newfp, &c->fp, sizeof(fingerprint));
            g_hash_table_insert(existing, newfp, NULL);
        }
    }

    rewrite_buffer.num++;
    rewrite_buffer.size += c->size;

    if (rewrite_buffer.num >= destor.rewrite_algorithm[1]) {
        assert(rewrite_buffer.num == destor.rewrite_algorithm[1]);
        return 1;
    }

    return 0;
}

void recordFP(gpointer key, gpointer value, int* user_data){
    struct fingerprint* fp = (struct fingerprint*)malloc(sizeof(fingerprint));
    memcpy(fp, key, sizeof(fingerprint));
    g_hash_table_insert(existing, fp, NULL);
}

static void smr_segment_get_top() {

    /* Descending order */
    g_sequence_sort(rewrite_buffer.container_record_seq,
                    g_record_descmp_by_length, NULL);

    int length = g_sequence_get_length(rewrite_buffer.container_record_seq);
    int32_t num = length > destor.rewrite_capping_level ?
                  destor.rewrite_capping_level : length, i;
    GSequenceIter *iter = g_sequence_get_begin_iter(
            rewrite_buffer.container_record_seq);
    for (i = 0; i < num; i++) {
        assert(!g_sequence_iter_is_end(iter));
        struct containerRecord* record = g_sequence_get(iter);
        struct containerRecord* r = (struct containerRecord*) malloc(
                sizeof(struct containerRecord));
        memcpy(r, record, sizeof(struct containerRecord));
        r->out_of_order = 0;
        g_hash_table_insert(top, &r->cid, r);

        if(record->cid <= getLatestCID()){
            struct containerMeta* cm = retrieve_container_meta_by_id(record->cid);
            g_hash_table_foreach(cm->map, recordFP, NULL);
            free_container_meta(cm);
        }

        iter = g_sequence_iter_next(iter);
    }

    VERBOSE("Rewrite phase: Select Top-%d in %d containers", num, length);

    g_sequence_sort(rewrite_buffer.container_record_seq, g_record_cmp_by_id, NULL);
}

/*
 * We first assemble a fixed-sized buffer of pending chunks.
 * Then, counting container utilization in the buffer and sorting.
 * The pending chunks in containers of most low utilization are fragmentation.
 * The main drawback of capping,
 * is that capping overlook the relationship of consecutive buffers.
 */
void *smr_rewrite(void* arg) {
    top = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, free);
    existing = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, free, NULL);

    while (1) {
        struct chunk *c = sync_queue_pop(dedup_queue);

        if (c == NULL)
            break;

        TIMER_DECLARE(1);
        TIMER_BEGIN(1);
        if (!rewrite_buffer_push_smr(c)) {
            TIMER_END(1, jcr.rewrite_time);
            continue;
        }

        smr_segment_get_top();

        while ((c = rewrite_buffer_pop())) {
            if (!CHECK_CHUNK(c,	CHUNK_FILE_START)
                && !CHECK_CHUNK(c, CHUNK_FILE_END)
                && !CHECK_CHUNK(c, CHUNK_SEGMENT_START)
                && !CHECK_CHUNK(c, CHUNK_SEGMENT_END)
                && CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
                if (g_hash_table_lookup(top, &c->id) == NULL) {
                    /* not in TOP */
                    SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
                    VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
                            chunk_num, c->id);
                }
                chunk_num++;
            }
            TIMER_END(1, jcr.rewrite_time);
            sync_queue_push(rewrite_queue, c);
            TIMER_BEGIN(1);
        }

        g_hash_table_remove_all(top);

    }

    smr_segment_get_top();

    struct chunk *c;
    while ((c = rewrite_buffer_pop())) {
        if (!CHECK_CHUNK(c,	CHUNK_FILE_START) && !CHECK_CHUNK(c, CHUNK_FILE_END)
            && !CHECK_CHUNK(c, CHUNK_SEGMENT_START) && !CHECK_CHUNK(c, CHUNK_SEGMENT_END)) {
            if (g_hash_table_lookup(top, &c->id) == NULL) {
                /* not in TOP */
                SET_CHUNK(c, CHUNK_OUT_OF_ORDER);
                VERBOSE("Rewrite phase: %lldth chunk is in out-of-order container %lld",
                        chunk_num, c->id);
            }
            chunk_num++;
        }
        sync_queue_push(rewrite_queue, c);
    }

    g_hash_table_remove_all(top);
    g_hash_table_remove_all(existing);

    sync_queue_term(rewrite_queue);

    return NULL;
}

