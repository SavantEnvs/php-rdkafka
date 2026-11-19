/*
 * libFuzzer harness: PHP embed SAPI with the (sanitized) rdkafka extension linked in.
 *
 * The PHP engine starts once (LLVMFuzzerInitialize). Each input runs in its own PHP
 * request: the raw bytes are bound to the PHP global $data and a fixed driver script
 * (mayhem/harness/<target>.php, compiled in as DRIVER_SRC) decodes them into argument
 * values for the RdKafka\* API. Request shutdown destroys every object the input
 * created, so no state is carried between inputs. PHP exceptions are caught inside the
 * driver; PHP fatal errors unwind through the engine's own zend_try/zend_end_try.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <threads.h>
#include <sapi/embed/php_embed.h>
#include "php_rdkafka.h"

#include DRIVER_HEADER /* defines: static const char DRIVER_SRC[] */

extern zend_module_entry rdkafka_module_entry;

/*
 * librdkafka starts its threads with C11 thrd_create(). glibc's thrd_create calls its
 * internal pthread_create directly, past ASan's pthread_create interceptor, so ASan never
 * registers those threads and never runs its thread-exit cleanup for them: every one
 * that touches librdkafka's __thread variables leaves one 4 KB dynamic-TLS page behind
 * (2 threads per Producer, 5 with the mock cluster), and RSS grows ~8-20 KB per input
 * for as long as the fuzzer runs. Routing thrd_create through the intercepted
 * pthread_create makes these ordinary ASan-known threads (same start routine, same
 * int result through thrd_join/thrd_exit; glibc's thrd_t is pthread_t).
 */
struct thrd_start_ctx { thrd_start_t fn; void *arg; };

static void *thrd_start_trampoline(void *p) {
    struct thrd_start_ctx ctx = *(struct thrd_start_ctx *)p;
    free(p);
    return (void *)(intptr_t)ctx.fn(ctx.arg);
}

int thrd_create(thrd_t *thr, thrd_start_t fn, void *arg) {
    struct thrd_start_ctx *ctx = malloc(sizeof *ctx);
    if (!ctx) return thrd_nomem;
    ctx->fn = fn;
    ctx->arg = arg;
    int rc = pthread_create((pthread_t *)thr, NULL, thrd_start_trampoline, ctx);
    if (rc != 0) {
        free(ctx);
        return rc == EAGAIN || rc == ENOMEM ? thrd_nomem : thrd_error;
    }
    return thrd_success;
}

static size_t quiet_ub_write(const char *str, size_t len) { (void)str; return len; }
static void quiet_log(const char *msg, int syslog_type) { (void)msg; (void)syslog_type; }
static void quiet_flush(void *ctx) { (void)ctx; }

static int startup_with_rdkafka(sapi_module_struct *sf) {
    return php_module_startup(sf, &rdkafka_module_entry);
}

int LLVMFuzzerInitialize(int *argc, char ***argv) {
    (void)argc; (void)argv;
    /* Use the system allocator so ASan sees PHP heap objects individually. */
    setenv("USE_ZEND_ALLOC", "0", 1);
    php_embed_module.startup = startup_with_rdkafka;
    php_embed_module.ub_write = quiet_ub_write;
    php_embed_module.log_message = quiet_log;
    php_embed_module.flush = quiet_flush;
    php_embed_module.ini_entries = "display_errors=0\nlog_errors=0\nmemory_limit=256M\nmax_execution_time=0\n";
    /* static: the SAPI keeps argv in SG(request_info) for every later request */
    static char *args[] = {"php_fuzz", NULL};
    if (php_embed_init(1, args) == FAILURE) abort();
    php_request_shutdown(NULL);
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (php_request_startup() == FAILURE) abort();
    zend_first_try {
        zval zdata;
        ZVAL_STRINGL(&zdata, (const char *)data, size);
        zend_hash_str_update(&EG(symbol_table), "data", sizeof("data") - 1, &zdata);
        zend_eval_stringl((char *)DRIVER_SRC, sizeof(DRIVER_SRC) - 1, NULL, "fuzz_driver");
    } zend_end_try();
    php_request_shutdown(NULL);
    return 0;
}
