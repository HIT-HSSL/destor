/*
 * delete_server.c
 *
 *  Created on: Jun 21, 2012
 *      Author: fumin
 */
#include <utils/sync_queue.h>
#include <stdbool.h>
#include "destor.h"
#include "storage/containerstore.h"
#include "recipe/recipestore.h"
#include "index/index.h"
#include "cma.h"
#include "jcr.h"

SyncQueue *delete_recipe_queue;
SyncQueue *migrate_data_queue;
GHashTable* global_gc_HashTable;
GHashTable *invalid_containers;
bool endFlag;

/* A simple wrap.
 * Just to make the interfaces of the index module more consistent.
 */
static inline void delete_an_entry(fingerprint *fp, int64_t *id){
	index_delete(fp, *id);
}

static inline void add_an_entry(fingerprint* fp, int64_t* id){
    struct chunk* c = new_chunk(0);
    memcpy(&c->fp, fp, sizeof(fingerprint));
    c->id = id;
    sync_queue_push(delete_recipe_queue, c);
}

static void* read_recipe_for_deletion(void *arg) {
    struct backupVersion* bv = (struct backupVersion*)arg;

    int i, j, k;
    for (i = 0; i < bv->number_of_files; i++) {



//        struct fileRecipeMeta *r = read_next_file_recipe_meta(bv);

        struct chunk *c = new_chunk(0);
        SET_CHUNK(c, CHUNK_FILE_START);

        sync_queue_push(delete_recipe_queue, c);

        struct segmentRecipe* sr;
        while((sr=read_next_segment(bv))){
            segment_recipe_foreach(sr, add_an_entry, &sr->id);
            int64_t* r = (int64_t*)malloc(sizeof(int64_t));
            *r = sr->id;
            g_hash_table_insert(invalid_containers, r, r);
        }

//        for (j = 0; j < r->chunknum; j++) {
//            struct chunkPointer* cp = read_next_n_chunk_pointers(bv, 1, &k);
//
//            struct chunk* c = new_chunk(0);
//            memcpy(&c->fp, &cp->fp, sizeof(fingerprint));
//            c->size = cp->size;
//            c->id = cp->id;
//
//
//            sync_queue_push(delete_recipe_queue, c);
//            free(cp);
//        }

        c = new_chunk(0);
        SET_CHUNK(c, CHUNK_FILE_END);
        sync_queue_push(delete_recipe_queue, c);

//        free_file_recipe_meta(r);
    }

    sync_queue_term(delete_recipe_queue);
    return NULL;
}

struct GCHashEntry{
    uint64_t cid;
    Queue* container_queue;
};

void free_chunk_void(void* c) {
    struct chunk* ck = (struct chunk*)c;
    if (ck->data) {
        free(ck->data);
        ck->data = NULL;
    }
    free(ck);
}

void destructor(gpointer ptr){
    struct GCHashEntry* eptr = (struct GCHashEntry*)ptr;
    queue_free(eptr->container_queue, free_chunk_void);
    free(ptr);
}

static void* gether_fingerprint_for_deletion(void *arg) {
    struct chunk* c;
    global_gc_HashTable = g_hash_table_new_full(g_int64_hash, g_int64_equal, NULL, destructor);

    while(c = sync_queue_pop(delete_recipe_queue)){
        if (CHECK_CHUNK(c, CHUNK_FILE_START)) {
            free_chunk(c);
            continue;
        }
        if (CHECK_CHUNK(c, CHUNK_FILE_END)) {
            free_chunk(c);
            endFlag = true;
            break;
        }
        struct GCHashEntry* gcHashEntry = g_hash_table_lookup(global_gc_HashTable, &c->id);
        if(gcHashEntry == NULL){
            struct GCHashEntry entry;
            entry.cid = c->id;
            entry.container_queue = queue_new();
            fingerprint* fp = (fingerprint*)malloc(sizeof(fingerprint));
            memcpy(fp, c->fp, sizeof(fingerprint));
            queue_push(entry.container_queue, fp);
            g_hash_table_insert(global_gc_HashTable, &entry.cid, &entry);
        }else{
            fingerprint* fp = (fingerprint*)malloc(sizeof(fingerprint));
            memcpy(fp, c->fp, sizeof(fingerprint));
            queue_push(gcHashEntry->container_queue, fp);
        }
        free_chunk(c);
    }
    return NULL;
}

struct metaEntry {
    int32_t off;
    int32_t len;
    fingerprint fp;
};

void chunk_filter(void* item, void* user_data){
    GHashTable * gHashTable = (GHashTable*)user_data;
    if(g_hash_table_contains(gHashTable, item)){
        g_hash_table_remove(gHashTable, item);
    }
}

void chunk_migrate(gpointer key, gpointer value, gpointer user_data){
    struct container* con = (struct container*)user_data;
    struct metaEntry* metaEntry = (struct metaEntry*)value;
    struct chunk* c = new_chunk(metaEntry->len);

    memcpy(c->fp, key, sizeof(fingerprint));
    c->size = metaEntry->len;
    memcpy(c->data, con->data + metaEntry->off, metaEntry->len);

    sync_queue_push(migrate_data_queue, c);
}

void read_container_filter(gpointer key, gpointer value, gpointer user_data){
    struct GCHashEntry* e = (struct GCHashEntry*)value;
    struct container* con = retrieve_container_by_id(e->cid);

    container_meta_foreach(&con->meta, delete_an_entry, &e->cid);

    queue_foreach(e->container_queue, chunk_filter, con->meta.map);
    g_hash_table_foreach(con->meta.map, chunk_migrate, con);
    free_container(con);
}

static void* load_container_for_deletion(void *arg) {
    struct chunk* c = new_chunk(0);
    SET_CHUNK(c, CHUNK_FILE_START);
    sync_queue_push(migrate_data_queue, c);

    g_hash_table_foreach(global_gc_HashTable, read_container_filter, NULL);

    c = new_chunk(0);
    SET_CHUNK(c, CHUNK_FILE_END);
    sync_queue_push(migrate_data_queue, c);

    return NULL;
}

static void* write_container_for_deletion(void *arg) {
    struct chunk* c;
    struct container* con;
    GSequence * gseq;
    GHashTable * features;
    int32_t seq_count = 0;
    while(c = sync_queue_pop(migrate_data_queue)){
        if (CHECK_CHUNK(c, CHUNK_FILE_START)) {
            con = create_container();
            gseq = g_sequence_new(free_chunk);
            seq_count = 0;
            features = g_hash_table_new_full(g_feature_hash, g_feature_equal, free, NULL);

            free_chunk(c);
            continue;
        }
        if (CHECK_CHUNK(c, CHUNK_FILE_END)) {
            free_chunk(c);

            write_container_async(con);


            features = sampling(gseq, seq_count);
            GSequenceIter* iter = g_sequence_get_begin_iter(gseq);
            while(!g_sequence_iter_is_end(iter)){

                struct chunk* ck = g_sequence_get(iter);
                fingerprint *ft = malloc(sizeof(fingerprint));
                memcpy(ft, &ck->fp, sizeof(fingerprint));
                g_hash_table_insert(features, ft, NULL);

                iter = g_sequence_iter_next(iter);
            }
            index_update(features, seq_count);

            g_sequence_free(gseq);
            g_hash_table_destroy(features);

            container_store_sync();

            endFlag = true;
            break;
        }

        if (container_overflow(con, c->size)) {
            write_container_async(con);


            features = sampling(gseq, seq_count);
            GSequenceIter* iter = g_sequence_get_begin_iter(gseq);
            while(!g_sequence_iter_is_end(iter)){

                struct chunk* ck = g_sequence_get(iter);
                fingerprint *ft = malloc(sizeof(fingerprint));
                memcpy(ft, &ck->fp, sizeof(fingerprint));
                g_hash_table_insert(features, ft, NULL);

                iter = g_sequence_iter_next(iter);
            }
            index_update(features, seq_count);

            g_sequence_free(gseq);
            g_hash_table_destroy(features);

            seq_count = 0;
            gseq = g_sequence_new(free_chunk);
            features = g_hash_table_new_full(g_feature_hash, g_feature_equal, free, NULL);
            con = create_container();
        }

        add_chunk_to_container(con, c);
        g_sequence_append(gseq, c);
        seq_count++;



    }

    return NULL;
}

/*
 * We assume a FIFO order of deleting backup, namely the oldest backup is deleted first.
 */
void do_delete(int jobid) {

	invalid_containers = trunc_manifest(jobid);

	init_index();
	init_recipe_store();
	init_container_store();

	struct backupVersion* backupVersion = open_backup_version(jobid);

    delete_recipe_queue = sync_queue_new(100);
    pthread_t read_t, build_t, load_t, write_t;
    endFlag = false;
    pthread_create(&read_t, NULL, read_recipe_for_deletion, backupVersion);
    pthread_create(&build_t, NULL, gether_fingerprint_for_deletion, NULL);
    do{
        usleep(100);
    }while(endFlag);
    endFlag = false;
    migrate_data_queue = sync_queue_new(100);
    pthread_create(&load_t, NULL, load_container_for_deletion, NULL);
    pthread_create(&write_t, NULL, write_container_for_deletion, NULL);
    do{
        usleep(100);
    }while(endFlag);

	/* Delete the invalid entries in the key-value store */
	if(destor.index_category[1] == INDEX_CATEGORY_PHYSICAL_LOCALITY){

		struct backupVersion* bv = open_backup_version(jobid);

		/* The entries pointing to Invalid Containers are invalid. */
		GHashTableIter iter;
		gpointer key, value;
		g_hash_table_iter_init(&iter, invalid_containers);
		while(g_hash_table_iter_next(&iter, &key, &value)){
			containerid id = *(containerid*)key;
			NOTICE("Reclaim container %lld", id);
			struct containerMeta* cm = retrieve_container_meta_by_id(id);

			container_meta_foreach(cm, delete_an_entry, &id);

			free_container_meta(cm);
		}

		bv->deleted = 1;
		update_backup_version(bv);
		free_backup_version(bv);

	}else if(destor.index_category[1] == INDEX_CATEGORY_LOGICAL_LOCALITY){
		/* Ideally, the entries pointing to segments in backup versions of a 'bv_num' less than 'jobid' are invalid. */
		/* (For simplicity) Since a FIFO order is given, we only need to remove the IDs exactly matched 'bv_num'. */
		struct backupVersion* bv = open_backup_version(jobid);

//		struct segmentRecipe* sr;
//		while((sr=read_next_segment(bv))){
//			segment_recipe_foreach(sr, delete_an_entry, &sr->id);
//		}

		bv->deleted = 1;
		update_backup_version(bv);
		free_backup_version(bv);

	}else{
		WARNING("Invalid index type");
		exit(1);
	}

	close_container_store();
	close_recipe_store();
	close_index();

	char logfile[] = "delete.log";
	FILE *fp = fopen(logfile, "a");
	/*
	 * ID of the job we delete,
	 * number of live containers,
	 * memory footprint
	 */
	fprintf(fp, "%d %d %d\n",
			jobid,
			destor.live_container_num,
			destor.index_memory_footprint);

	fclose(fp);

	/* record the IDs of invalid containers */
	sds didfilepath = sdsdup(destor.working_directory);
	char s[128];
	sprintf(s, "recipes/delete_%d.id", jobid);
	didfilepath = sdscat(didfilepath, s);

	FILE*  didfile = fopen(didfilepath, "w");
	if(didfile){
		GHashTableIter iter;
		gpointer key, value;
		g_hash_table_iter_init(&iter, invalid_containers);
		while(g_hash_table_iter_next(&iter, &key, &value)){
			containerid id = *(containerid*)key;
			fprintf(didfile, "%lld\n", id);
		}

		fclose(didfile);
	}


	g_hash_table_destroy(invalid_containers);
}
