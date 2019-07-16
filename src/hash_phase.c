#include "destor.h"
#include "jcr.h"
#include "backup.h"
#include "mh_sha1.h"
#include "md5_mb.h"

static pthread_t hash_t;
static int64_t chunk_num;

static void* sha1_thread(void* arg) {
	char code[41];
	struct mh_sha1_ctx *ctx;
	MD5_HASH_CTX_MGR *msg;
	ctx = malloc(sizeof(struct mh_sha1_ctx));
	while (1) {
		struct chunk* c = sync_queue_pop(chunk_queue);

		if (c == NULL) {
			sync_queue_term(hash_queue);
			break;
		}

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
			sync_queue_push(hash_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		//SHA_CTX ctx;
		//SHA1_Init(&ctx);
		//SHA1_Update(&ctx, c->data, c->size);
		//SHA1_Final(c->fp, &ctx);

		mh_sha1_init(ctx);
		mh_sha1_update_avx512(ctx, c->data, c->size);
		mh_sha1_finalize_avx512(ctx, c->fp);
		
		//md5_ctx_mgr_init(msg);
		//result = md5_ctx_mgr_submit(msg, ctxptr, c->data, c->size, HASH_ENTIRE);
		//result = sha1_ctx_mgr_flush(msg);
		//memcpy(c->fp, result->job.result_digest, 20);
		TIMER_END(1, jcr.hash_time);

		hash2code(c->fp, code);
		code[40] = 0;
		VERBOSE("Hash phase: %ldth chunk identified by %s", chunk_num++, code);

		sync_queue_push(hash_queue, c);
	}
	return NULL;
}

void start_hash_phase() {
	hash_queue = sync_queue_new(100);
	pthread_create(&hash_t, NULL, sha1_thread, NULL);
}

void stop_hash_phase() {
	pthread_join(hash_t, NULL);
	NOTICE("hash phase stops successfully!");
}
