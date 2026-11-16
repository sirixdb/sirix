/* SPDX-License-Identifier: BSD-3-Clause
 * SirixDB native ABI 1.0 declaration contract. No implementation ships here yet.
 * Normative behavior: spec/NATIVE-embedded-c-api.md (NATIVE-101 through 501).
 * 64-bit targets only. Do not change packing. All byte lengths exclude NUL.
 */
#ifndef SIRIX_H
#define SIRIX_H

#include <stdint.h>

/* Define SIRIX_BUILD when building the DLL; SIRIX_FFI is for cffi preprocessing. */
#if defined(SIRIX_FFI)
#define SIRIX_API
#define SIRIX_CALL
#elif defined(_WIN32)
#if defined(SIRIX_BUILD)
#define SIRIX_API __declspec(dllexport)
#else
#define SIRIX_API __declspec(dllimport)
#endif
#define SIRIX_CALL __cdecl
#else
#define SIRIX_API __attribute__((visibility("default")))
#define SIRIX_CALL
#endif

#define SIRIX_ABI_MAJOR 1
#define SIRIX_ABI_MINOR 0
#define SIRIX_ABI_VERSION ((SIRIX_ABI_MAJOR << 16) | SIRIX_ABI_MINOR)

/* Stable status manifest. Never reuse a number, including after retirement. */
#define SIRIX_OK 0
#define SIRIX_EOF 1
#define SIRIX_INVALID_ARGUMENT 100
#define SIRIX_RANGE 101
#define SIRIX_INVALID_HANDLE 102
#define SIRIX_HANDLE_TYPE 103
#define SIRIX_WRONG_THREAD 104
#define SIRIX_WRONG_RUNTIME 105
#define SIRIX_NOT_FOUND 106
#define SIRIX_ALREADY_EXISTS 107
#define SIRIX_TYPE_MISMATCH 108
#define SIRIX_BUSY 109
#define SIRIX_LOCKED 110
#define SIRIX_READ_ONLY 111
#define SIRIX_REVISION_PRUNED 112
#define SIRIX_NOT_CONFIGURED 113
#define SIRIX_UNSUPPORTED 114
#define SIRIX_SYNTAX 115
#define SIRIX_QUERY 116
#define SIRIX_BUFFER_TOO_SMALL 117
#define SIRIX_LIMIT 118
#define SIRIX_CANCELLED 119
#define SIRIX_IO 120
#define SIRIX_CORRUPTION 121
#define SIRIX_OOM 122
#define SIRIX_TX_FAILED 123
#define SIRIX_COMMIT_OUTCOME_UNKNOWN 124
#define SIRIX_INTERNAL 125
#define SIRIX_ABI_MISMATCH 126
#define SIRIX_COMMITTED_WITH_ERROR 127
#define SIRIX_CLOSE_FAILED 128
#define SIRIX_RUNTIME_POISONED 129

#define SIRIX_CAP_JSON 1
#define SIRIX_CAP_XML 2
#define SIRIX_CAP_JSONIQ 4
#define SIRIX_CAP_XQUERY 8
#define SIRIX_CAP_TEMPORAL 16
#define SIRIX_CAP_DIFF 32
#define SIRIX_CAP_HISTORY 64
#define SIRIX_CAP_BOUNDED_PULL 128
#define SIRIX_CAP_REQUIRED_1_0 255

#define SIRIX_ERROR_TRUNCATED 1
#define SIRIX_ERROR_TX_CLEANUP_ONLY 2
#define SIRIX_ERROR_RUNTIME_POISONED 4
#define SIRIX_ERROR_HANDLE_CONSUMED 8
#define SIRIX_COMMIT_NONE 0
#define SIRIX_NOT_COMMITTED 1
#define SIRIX_COMMITTED 2
#define SIRIX_COMMIT_UNKNOWN 3

#define SIRIX_MODEL_JSON 1
#define SIRIX_MODEL_XML 2
#define SIRIX_ACCESS_READ_WRITE 1
#define SIRIX_ACCESS_READ_ONLY 2
#define SIRIX_BACKEND_FILE_CHANNEL 1
#define SIRIX_VERSIONING_SLIDING_SNAPSHOT 1
#define SIRIX_SELECT_LATEST 1
#define SIRIX_SELECT_REVISION 2
#define SIRIX_SELECT_TIMESTAMP 3
#define SIRIX_LANGUAGE_JSONIQ 1
#define SIRIX_LANGUAGE_XQUERY 2

#define SIRIX_ACTION_REPLACE_DOCUMENT 1
#define SIRIX_ACTION_REPLACE_SUBTREE 2
#define SIRIX_ACTION_FIRST_CHILD 3
#define SIRIX_ACTION_LAST_CHILD 4
#define SIRIX_ACTION_LEFT_SIBLING 5
#define SIRIX_ACTION_RIGHT_SIBLING 6
#define SIRIX_ACTION_REMOVE 7

#define SIRIX_ITEM_JSON 1
#define SIRIX_ITEM_XML 2
#define SIRIX_ITEM_ATOMIC 3
#define SIRIX_ITEM_JSON_DIFF 4
#define SIRIX_ITEM_XML_DIFF 5
#define SIRIX_ITEM_HISTORY 6
#define SIRIX_CHUNK_BEGIN 1
#define SIRIX_CHUNK_END 2
#define SIRIX_DIFF_INCLUDE_DATA 1

#define SIRIX_VALUE_JSON 1
#define SIRIX_VALUE_STRING 2
#define SIRIX_VALUE_BOOLEAN 3
#define SIRIX_VALUE_INT64 4
#define SIRIX_VALUE_DECIMAL 5
#define SIRIX_VALUE_INSTANT 6
#define SIRIX_VALUE_XML 7
#define SIRIX_XML_DOCUMENT 1
#define SIRIX_XML_ELEMENT 2
#define SIRIX_XML_ATTRIBUTE 3
#define SIRIX_XML_NAMESPACE 4
#define SIRIX_XML_TEXT 5
#define SIRIX_XML_COMMENT 6
#define SIRIX_XML_PROCESSING_INSTRUCTION 7

#define SIRIX_DEFAULT_HEAP_BYTES 268435456
#define SIRIX_DEFAULT_ALLOCATOR_BYTES 268435456
#define SIRIX_DEFAULT_WORKSPACE_BYTES 67108864
#define SIRIX_DEFAULT_INPUT_BYTES 16777216
#define SIRIX_DEFAULT_CHUNK_BYTES 65536
#define SIRIX_DEFAULT_WORKERS 2
#define SIRIX_DEFAULT_MAX_DEPTH 1024

typedef int32_t sirix_status_t;
typedef uint64_t sirix_handle_t;

/* Initialize prefixes even for nested instant values. nanos < 1,000,000,000. */
typedef struct sirix_instant {
  uint32_t struct_size, abi_major;
  int64_t seconds;
  uint32_t nanos, reserved;
} sirix_instant_t;

typedef struct sirix_error {
  uint32_t struct_size, abi_major;
  int32_t code, detail_code;
  uint64_t revision;
  uint32_t commit_outcome, suppressed_count;
  uint32_t message_len, flags;
  char message[512];
} sirix_error_t;

typedef struct sirix_runtime_options {
  uint32_t struct_size, abi_major;
  uint64_t heap_bytes, allocator_bytes, query_workspace_bytes, max_input_bytes;
  uint64_t chunk_bytes;
  uint32_t worker_count, max_depth, flags, reserved;
} sirix_runtime_options_t;

typedef struct sirix_db_options {
  uint32_t struct_size, abi_major;
  uint32_t model, access, flags, reserved;
} sirix_db_options_t;

typedef struct sirix_resource_options {
  uint32_t struct_size, abi_major;
  uint32_t backend, versioning, path_summary, node_history, flags, reserved;
  const uint8_t *valid_from;
  uint64_t valid_from_len;
  const uint8_t *valid_to;
  uint64_t valid_to_len;
} sirix_resource_options_t;

typedef struct sirix_query_options {
  uint32_t struct_size, abi_major;
  uint32_t language, flags;
  uint64_t workspace_bytes, chunk_bytes;
  uint32_t max_depth, reserved;
} sirix_query_options_t;

typedef struct sirix_selector {
  uint32_t struct_size, abi_major;
  uint32_t kind, flags;
  uint64_t revision;
  sirix_instant_t instant;
} sirix_selector_t;

typedef struct sirix_chunk {
  uint32_t struct_size, abi_major;
  uint32_t item_kind, flags;
  uint64_t bytes_written, item_index;
} sirix_chunk_t;

/* Borrowed input fields; copied during the call. See NATIVE §3.3 per node kind. */
typedef struct sirix_xml_value {
  uint32_t struct_size, abi_major;
  uint32_t node_kind, flags;
  const uint8_t *namespace_uri;
  uint64_t namespace_uri_len;
  const uint8_t *local;
  uint64_t local_len;
  const uint8_t *prefix;
  uint64_t prefix_len;
  const uint8_t *value;
  uint64_t value_len;
} sirix_xml_value_t;

typedef struct sirix_value {
  uint32_t struct_size, abi_major;
  uint32_t kind, boolean_value;
  int64_t integer_value;
  sirix_instant_t instant;
  const uint8_t *data;
  uint64_t len;
  const sirix_xml_value_t *xml;
  uint64_t reserved;
} sirix_value_t;

/* Aggregate initializers; no functions, designated initializers or C++ objects. */
#define SIRIX_INSTANT_INIT {sizeof(sirix_instant_t), SIRIX_ABI_MAJOR, 0, 0, 0}
#define SIRIX_ERROR_INIT \
  {sizeof(sirix_error_t), SIRIX_ABI_MAJOR, 0, 0, 0, 0, 0, 0, 0, {0}}
#define SIRIX_RUNTIME_OPTIONS_INIT \
  {sizeof(sirix_runtime_options_t), SIRIX_ABI_MAJOR, SIRIX_DEFAULT_HEAP_BYTES, \
   SIRIX_DEFAULT_ALLOCATOR_BYTES, SIRIX_DEFAULT_WORKSPACE_BYTES, \
   SIRIX_DEFAULT_INPUT_BYTES, SIRIX_DEFAULT_CHUNK_BYTES, SIRIX_DEFAULT_WORKERS, \
   SIRIX_DEFAULT_MAX_DEPTH, 0, 0}
#define SIRIX_DB_OPTIONS_INIT \
  {sizeof(sirix_db_options_t), SIRIX_ABI_MAJOR, SIRIX_MODEL_JSON, SIRIX_ACCESS_READ_WRITE, 0, 0}
#define SIRIX_RESOURCE_OPTIONS_INIT \
  {sizeof(sirix_resource_options_t), SIRIX_ABI_MAJOR, SIRIX_BACKEND_FILE_CHANNEL, \
   SIRIX_VERSIONING_SLIDING_SNAPSHOT, 1, 1, 0, 0, 0, 0, 0, 0}
/* Explicit options default to JSONiq; set language=XQUERY for an XML transaction. */
#define SIRIX_QUERY_OPTIONS_INIT \
  {sizeof(sirix_query_options_t), SIRIX_ABI_MAJOR, SIRIX_LANGUAGE_JSONIQ, 0, 0, 0, 0, 0}
#define SIRIX_SELECTOR_INIT \
  {sizeof(sirix_selector_t), SIRIX_ABI_MAJOR, SIRIX_SELECT_LATEST, 0, 0, SIRIX_INSTANT_INIT}
#define SIRIX_CHUNK_INIT {sizeof(sirix_chunk_t), SIRIX_ABI_MAJOR, 0, 0, 0, 0}
#define SIRIX_XML_VALUE_INIT \
  {sizeof(sirix_xml_value_t), SIRIX_ABI_MAJOR, SIRIX_XML_ELEMENT, 0, 0, 0, 0, 0, 0, 0, 0, 0}
#define SIRIX_VALUE_INIT \
  {sizeof(sirix_value_t), SIRIX_ABI_MAJOR, SIRIX_VALUE_STRING, 0, 0, SIRIX_INSTANT_INIT, 0, 0, 0, 0}

#ifdef __cplusplus
extern "C" {
#endif

/* Infallible and callable before runtime creation. No pointer or runtime exposed. */
SIRIX_API uint32_t SIRIX_CALL sirix_abi_version(void);
SIRIX_API uint64_t SIRIX_CALL sirix_capabilities(void);

/* Every err is optional. Other outputs are required and initialized on failure.
 * Null options select published defaults. Caller bytes are never retained.
 * Parent close is BUSY while children live; see §3.2 for consuming error statuses.
 */
SIRIX_API sirix_status_t SIRIX_CALL sirix_runtime_create(
    const sirix_runtime_options_t *options, sirix_handle_t *runtime, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_thread_attach(
    sirix_handle_t runtime, sirix_handle_t *ctx, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_thread_detach(sirix_handle_t ctx, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_runtime_destroy(
    sirix_handle_t ctx, sirix_handle_t runtime, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_runtime_info(
    sirix_handle_t ctx, uint8_t *dst, uint64_t capacity, uint64_t *needed, sirix_error_t *err);

SIRIX_API sirix_status_t SIRIX_CALL sirix_database_create(
    sirix_handle_t ctx, const uint8_t *path, uint64_t len,
    const sirix_db_options_t *options, sirix_handle_t *db, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_database_open(
    sirix_handle_t ctx, const uint8_t *path, uint64_t len,
    const sirix_db_options_t *options, sirix_handle_t *db, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_resource_create(
    sirix_handle_t ctx, sirix_handle_t db, const uint8_t *name, uint64_t len,
    const sirix_resource_options_t *options, sirix_handle_t *res, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_resource_open(
    sirix_handle_t ctx, sirix_handle_t db, const uint8_t *name, uint64_t len,
    sirix_handle_t *res, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_resource_info(
    sirix_handle_t ctx, sirix_handle_t res, uint8_t *dst, uint64_t capacity,
    uint64_t *needed, sirix_error_t *err);

SIRIX_API sirix_status_t SIRIX_CALL sirix_read_begin(
    sirix_handle_t ctx, sirix_handle_t res, const sirix_selector_t *selector,
    sirix_handle_t *trx, uint64_t *revision, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_write_begin(
    sirix_handle_t ctx, sirix_handle_t res, sirix_handle_t *trx,
    uint64_t *base_revision, sirix_error_t *err);
/* REMOVE requires json=NULL,len=0 or xml=NULL. Other actions need payloads. */
SIRIX_API sirix_status_t SIRIX_CALL sirix_write_json(
    sirix_handle_t ctx, sirix_handle_t trx, uint64_t node_key, uint32_t action,
    const uint8_t *json, uint64_t len, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_write_xml(
    sirix_handle_t ctx, sirix_handle_t trx, uint64_t node_key, uint32_t action,
    const sirix_xml_value_t *xml, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_commit(
    sirix_handle_t ctx, sirix_handle_t trx, const uint8_t *message, uint64_t len,
    uint64_t *revision, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_rollback(
    sirix_handle_t ctx, sirix_handle_t trx, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_read_json(
    sirix_handle_t ctx, sirix_handle_t trx, uint64_t node_key,
    sirix_handle_t *cursor, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_read_xml(
    sirix_handle_t ctx, sirix_handle_t trx, uint64_t node_key,
    sirix_handle_t *cursor, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_bitemporal_open(
    sirix_handle_t ctx, sirix_handle_t res, const sirix_instant_t *transaction_time,
    const sirix_instant_t *valid_time, sirix_handle_t *cursor,
    uint64_t *revision, sirix_error_t *err);

SIRIX_API sirix_status_t SIRIX_CALL sirix_query_prepare(
    sirix_handle_t ctx, sirix_handle_t read_trx, const uint8_t *text, uint64_t len,
    const sirix_query_options_t *options, sirix_handle_t *query, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_query_bind(
    sirix_handle_t ctx, sirix_handle_t query, const uint8_t *namespace_uri, uint64_t uri_len,
    const uint8_t *local, uint64_t local_len, const sirix_value_t *value, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_query_execute(
    sirix_handle_t ctx, sirix_handle_t query, sirix_handle_t *cursor, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_diff_open(
    sirix_handle_t ctx, sirix_handle_t res, uint64_t old_revision, uint64_t new_revision,
    uint32_t flags, sirix_handle_t *cursor, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_history_open(
    sirix_handle_t ctx, sirix_handle_t res, uint64_t first_revision, uint64_t last_revision,
    sirix_handle_t *cursor, sirix_error_t *err);

/* Size probes on info calls do not advance. cursor_read capacity=0 does not advance.
 * A cancelled/failed cursor is a live child until closed. No borrowed result bytes.
 */
SIRIX_API sirix_status_t SIRIX_CALL sirix_cursor_read(
    sirix_handle_t ctx, sirix_handle_t cursor, uint8_t *dst, uint64_t capacity,
    sirix_chunk_t *chunk, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_cursor_item_info(
    sirix_handle_t ctx, sirix_handle_t cursor, uint8_t *dst, uint64_t capacity,
    uint64_t *needed, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_cursor_cancel(
    sirix_handle_t ctx, sirix_handle_t cursor, sirix_error_t *err);
SIRIX_API sirix_status_t SIRIX_CALL sirix_handle_close(
    sirix_handle_t ctx, sirix_handle_t handle, sirix_error_t *err);

#ifdef __cplusplus
}
#endif
#endif /* SIRIX_H */
