#include <a0/alloc.h>
#include <a0/arena.h>
#include <a0/common.h>
#include <a0/errno.h>
#include <a0/transport.h>

#include <errno.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <glib-2.0/glib.h>

#include "atomic.h"
#include "ftx.h"
#include "macros.h"
#include "mtx.h"
#include "pmparser.h"

// Use MONOTONIC_COARSE to avoid syscall cost at the expense of reduced resolution.
// On our systems, the coarse clock has a 4ms resolution, which is still good enough
// for >200Hz timings.
#define A0_PERF_CLOCK_SOURCE CLOCK_MONOTONIC_COARSE

// Mutex covering a0_global_reverse_mmap.
// This initial value is equivalent to having called a0_mutex_init.
static a0_mtx_t a0_global_reverse_mmap_mu = (a0_mtx_t){0};

// Hash table for caching memory location<->mapped file associations.
static GHashTable* a0_global_reverse_mmap = NULL;

typedef uintptr_t transport_off_t;  // ptr offset from start of the arena.

typedef struct a0_transport_state_s {
  uint64_t seq_low;
  uint64_t seq_high;
  transport_off_t off_head;
  transport_off_t off_tail;
} a0_transport_state_t;

typedef struct a0_transport_hdr_s {
  bool init_started;
  bool init_completed;

  a0_mtx_t mu;

  a0_ftx_t ftxcv;
  uint32_t next_ftxcv_tkn;
  bool has_notify_listener;

  a0_transport_state_t state_pages[2];
  uint32_t committed_page_idx;

  size_t arena_size;
  size_t metadata_size;
} a0_transport_hdr_t;

A0_STATIC_INLINE
a0_transport_state_t* a0_transport_committed_page(a0_locked_transport_t lk) {
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk.transport->_arena.ptr;
  return &hdr->state_pages[hdr->committed_page_idx];
}

A0_STATIC_INLINE
a0_transport_state_t* a0_transport_working_page(a0_locked_transport_t lk) {
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk.transport->_arena.ptr;
  return &hdr->state_pages[!hdr->committed_page_idx];
}

A0_STATIC_INLINE
transport_off_t a0_max_align(transport_off_t off) {
  return ((off + alignof(max_align_t) - 1) & ~(alignof(max_align_t) - 1));
}

A0_STATIC_INLINE
transport_off_t a0_transport_metadata_off() {
  return a0_max_align(sizeof(a0_transport_hdr_t));
}

A0_STATIC_INLINE
transport_off_t a0_transport_workspace_off(a0_transport_hdr_t* hdr) {
  return a0_max_align(a0_transport_metadata_off() + hdr->metadata_size);
}

A0_STATIC_INLINE
errno_t a0_transport_init_create(a0_transport_t* transport,
                                 a0_transport_init_status_t* status_out,
                                 a0_locked_transport_t* lk_out) {
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)transport->_arena.ptr;

  hdr->arena_size = transport->_arena.size;
  A0_RETURN_ERR_ON_ERR(a0_mtx_init(&hdr->mu));

  A0_RETURN_ERR_ON_ERR(a0_transport_lock(transport, lk_out));
  A0_TSAN_ANNOTATE_HAPPENS_BEFORE(&hdr->init_completed);
  a0_atomic_store(&hdr->init_completed, true);
  *status_out = A0_TRANSPORT_CREATED;

  return A0_OK;
}

errno_t a0_transport_init(a0_transport_t* transport,
                          a0_arena_t arena,
                          a0_transport_init_status_t* status_out,
                          a0_locked_transport_t* lk_out) {
  // The arena is expected to be either:
  // 1) all null bytes.
  //    this is guaranteed by ftruncate, as is used in a0/file_arena.h
  // 2) a pre-initialized buffer.
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)arena.ptr;

  memset(transport, 0, sizeof(a0_transport_t));
  transport->_arena = arena;

  if (!a0_cas(&hdr->init_started, 0, 1)) {
    return a0_transport_init_create(transport, status_out, lk_out);
  }

  // Spin until transport is initialized.
  while (A0_UNLIKELY(!a0_atomic_load(&hdr->init_completed))) {
    a0_spin();
  }
  A0_TSAN_ANNOTATE_HAPPENS_AFTER(&hdr->init_completed);
  A0_RETURN_ERR_ON_ERR(a0_transport_lock(transport, lk_out));
  *status_out = A0_TRANSPORT_CONNECTED;
  return A0_OK;
}

errno_t a0_transport_init_metadata(a0_locked_transport_t lk, size_t metadata_size) {
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk.transport->_arena.ptr;

  bool empty;
  A0_RETURN_ERR_ON_ERR(a0_transport_empty(lk, &empty));
  if (!empty) {
    return EACCES;
  }

  if (sizeof(a0_transport_hdr_t) + metadata_size + 64 >= (uint64_t)lk.transport->_arena.size) {
    return ENOMEM;
  }

  hdr->metadata_size = metadata_size;

  return A0_OK;
}

A0_STATIC_INLINE
void a0_wait_for_notify(a0_locked_transport_t* lk) {
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk->transport->_arena.ptr;

  uint32_t key = lk->transport->_lk_tkn;
  hdr->ftxcv = key;
  hdr->has_notify_listener = true;

  a0_transport_unlock(lk);
  a0_ftx_wait(&hdr->ftxcv, key, NULL);
  a0_transport_lock(lk->transport, lk);
}

A0_STATIC_INLINE
void a0_schedule_notify(a0_locked_transport_t lk) {
  lk.transport->_should_notify = true;
}

errno_t a0_transport_close(a0_transport_t* transport) {
  a0_locked_transport_t lk;
  a0_transport_lock(transport, &lk);

  transport->_closing = true;
  a0_schedule_notify(lk);

  while (transport->_await_cnt) {
    a0_wait_for_notify(&lk);
  }

  a0_transport_unlock(&lk);

  return A0_OK;
}


// Given a memory pointer (the transport arena pointer by convention)
// return the OS path of the memory mapped file backing it. Returns
// "Unknown" if the address is unknown in the memory map or there's
// a problem reading the memory map. Returns "" if the address is not
// backed by a mmapped file-like object.
//
// This function attempts to cache its results in a global cache to
// enable fast return. Otherwise it parses the process /proc/self/maps
// file to determine the mmap status.
//
// This function is threadsafe.
//
// Note: This cache currently has no invalidation or eviction policy.
A0_STATIC_INLINE
char* a0_reverse_map_arena_to_path(a0_transport_hdr_t* arena) {
  // Note: This allocates a hash table and all the values and
  // makes no attempt to clean any of it up. This memory is intended
  // to live for the life of the program and thus freeing is not a priority.
  // This is only valid if the assumption that transports are rarely
  // destroyed holds.

  // Lock the memory map.
  errno_t lock_status = a0_mtx_lock(&a0_global_reverse_mmap_mu);
  if (A0_UNLIKELY(lock_status == EOWNERDEAD)) {
    lock_status = a0_mtx_consistent(&a0_global_reverse_mmap_mu);
  }

  if (a0_global_reverse_mmap == NULL) {
    a0_global_reverse_mmap = g_hash_table_new(NULL, NULL);
  }
  char* maybe_path = g_hash_table_lookup(a0_global_reverse_mmap, arena);
  if (maybe_path) {
    // Fast-path: We already have an entry. Unlock the map and return.
    a0_mtx_unlock(&a0_global_reverse_mmap_mu);
    return maybe_path;
  }

  // Fetch the process map.  We do this every time if we've missed in the
  // hash map in case there have been subsequent mmappings.
  procmaps_iterator* pmparse = pmparser_parse(-1);
  char* pathname = "Unknown";
  if (!pmparse) {
    // Bail out if we were unable to read the memory map.
    // Don't cache the Unknown result.
    a0_mtx_unlock(&a0_global_reverse_mmap_mu);
    return pathname;
  }

  procmaps_struct* entry = NULL;
  while ((entry = pmparser_next(pmparse))) {
    // Look for an address in the range. We expect to find an mmapped
    // file path here.
    if (entry->addr_start <= (void*)(arena) && entry->addr_end > (void*)(arena)) {
      pathname = entry->pathname;
      break;
    }
  }
  // Fall-through on not-found will have pathname == "Unknown".  We will cache this
  // version of unknown, since the transport might be in-memory for test purposes
  // and we don't want to continually scan the memory map.

  // Note: this string allocation has no associated free. This cache currently
  // persists for the life of the program.
  char* result = strdup(pathname);
  g_hash_table_insert(a0_global_reverse_mmap, arena, result);

  // Clean up the parser. Needs to be done after the pathname dup.
  pmparser_free(pmparse);

  a0_mtx_unlock(&a0_global_reverse_mmap_mu);
  return result;
}

// Given a start time and end time, check to see if the timedelta is > the given
// threshold (a number of nanoseconds).
// If so:
//  * Fetch the backing mmapped file path for the given @p arena
//  * Emit a warning message on stderr with the path, the @p preamble and
//    the measured and threshold timings.
// If not:
//  * Do nothing and return.
A0_STATIC_INLINE
void a0_warn_if_past_threshold(struct timespec* start, struct timespec* end, int64_t threshold_ns,
                               const char* preamble, a0_transport_hdr_t* arena) {
  int64_t seconds = end->tv_sec - start->tv_sec;
  int64_t nanoseconds = (seconds * 1000000000) + (end->tv_nsec - start->tv_nsec);

  if (A0_UNLIKELY(nanoseconds > threshold_ns) && (threshold_ns != 0)) {
    char* arena_file = a0_reverse_map_arena_to_path(arena);
    const double kMsPerNs = 1e-6;
    fprintf(stderr, "Warning: [%s] %s %.3fms. Above threshold of %.3fms\n", arena_file, preamble, nanoseconds * kMsPerNs, threshold_ns * kMsPerNs);
  }
}

A0_STATIC_INLINE
void a0_warn_if_long_acquire(struct timespec* start, struct timespec* end, a0_transport_hdr_t* arena) {
  const int64_t kDefaultAcquireWarnNs = 100000000;  // 100ms via rough engineering judgement. 10Hz is a nominal value in TWA systems.
  // Static here to not cost a bunch of time on every entry.
  static int64_t kAcquireWarnNs = -1;
  if (A0_UNLIKELY(kAcquireWarnNs == -1)) {
    kAcquireWarnNs = (getenv("A0_LOCK_ACQUIRE_WARN_NS")) ? atoll(getenv("A0_LOCK_ACQUIRE_WARN_NS")) : kDefaultAcquireWarnNs;
  }

  a0_warn_if_past_threshold(start, end, kAcquireWarnNs, "A0 Transport lock acquisition took", arena);
}

A0_STATIC_INLINE
void a0_warn_if_long_hold(struct timespec* start, struct timespec* end, a0_transport_hdr_t* arena) {
  const int64_t kDefaultHoldWarnNs = 100000000;  // 100ms via rough engineering judgement. 10Hz is a nominal value in TWA systems.
  // Static here to not cost a bunch of time on every entry.
  static int64_t kHoldWarnNs = -1;
  if (A0_UNLIKELY(kHoldWarnNs == -1)) {
    kHoldWarnNs = (getenv("A0_LOCK_HOLD_WARN_NS")) ? atoll(getenv("A0_LOCK_HOLD_WARN_NS")) : kDefaultHoldWarnNs;
  }

  a0_warn_if_past_threshold(start, end, kHoldWarnNs, "A0 Transport lock held for", arena);
}

errno_t a0_transport_lock(a0_transport_t* transport, a0_locked_transport_t* lk_out) {
  struct timespec start_acquire;
  A0_ASSERT_OK(clock_gettime(A0_PERF_CLOCK_SOURCE, &start_acquire), "Failed clock_gettime");

  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)transport->_arena.ptr;

  lk_out->transport = transport;

  errno_t lock_status = a0_mtx_lock(&hdr->mu);
  if (A0_UNLIKELY(lock_status == EOWNERDEAD)) {
    // The data is always consistent by design.
    lock_status = a0_mtx_consistent(&hdr->mu);
    a0_schedule_notify(*lk_out);
  }

  // Measure lock aquisition time and warn if necessary.
  A0_ASSERT_OK(clock_gettime(A0_PERF_CLOCK_SOURCE, &(lk_out->lock_monotime)), "Failed clock_gettime");
  a0_warn_if_long_acquire(&start_acquire, &(lk_out->lock_monotime), hdr);

  lk_out->transport->_lk_tkn = a0_atomic_inc_fetch(&hdr->next_ftxcv_tkn);

  // Clear any incomplete changes.
  *a0_transport_working_page(*lk_out) = *a0_transport_committed_page(*lk_out);
  lk_out->transport->_should_notify = false;

  return lock_status;
}

errno_t a0_transport_unlock(a0_locked_transport_t* lk) {
  *a0_transport_working_page(*lk) = *a0_transport_committed_page(*lk);
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk->transport->_arena.ptr;
  if (hdr->has_notify_listener && lk->transport->_should_notify) {
    // wait_for_notify unlocks (using this function) before starting the futex_wait.
    // In all other cases, futex_broadcast should clear has_notify_listener.
    // The following line effectively checks whether the unlock is part of wait_for_notify.
    // TODO(lshamis): This code is piped weird and should be cleaned up.
    hdr->has_notify_listener = (hdr->ftxcv == lk->transport->_lk_tkn);
    hdr->ftxcv = lk->transport->_lk_tkn;
    a0_ftx_broadcast(&hdr->ftxcv);
  }
  a0_mtx_unlock(&hdr->mu);

  // Note: This measures held time very liberally. i.e.: The actual held time is some amount less
  // than this. A conservative measurement would have the time capture before the a0_mtx_unlock
  // call.  The choice of conservative vs liberal measurement here is somewhat arbitrary since
  // the warning time threshold is roughly chosen to be so high that this distinction should be
  // meaningless and thus getting warned for actual holds that are close-but-not-over the threshold
  // is preferable to under-measuring held timings.
  struct timespec release_monotime;
  A0_ASSERT_OK(clock_gettime(A0_PERF_CLOCK_SOURCE, &release_monotime), "Failed clock_gettime");
  a0_warn_if_long_hold(&(lk->lock_monotime), &release_monotime, hdr);

  return A0_OK;
}

errno_t a0_transport_metadata(a0_locked_transport_t lk, a0_buf_t* metadata_out) {
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk.transport->_arena.ptr;
  metadata_out->ptr = (uint8_t*)hdr + a0_transport_metadata_off();
  metadata_out->size = hdr->metadata_size;
  return A0_OK;
}

errno_t a0_transport_empty(a0_locked_transport_t lk, bool* out) {
  a0_transport_state_t* working_page = a0_transport_working_page(lk);
  *out = !working_page->seq_high | (working_page->seq_low > working_page->seq_high);
  return A0_OK;
}

errno_t a0_transport_nonempty(a0_locked_transport_t lk, bool* out) {
  errno_t err = a0_transport_empty(lk, out);
  *out = !*out;
  return err;
}

errno_t a0_transport_ptr_valid(a0_locked_transport_t lk, bool* out) {
  a0_transport_state_t* working_page = a0_transport_working_page(lk);
  *out = (working_page->seq_low <= lk.transport->_seq) &&
         (lk.transport->_seq <= working_page->seq_high);
  return A0_OK;
}

errno_t a0_transport_jump_head(a0_locked_transport_t lk) {
  a0_transport_state_t* state = a0_transport_working_page(lk);

  bool empty;
  A0_RETURN_ERR_ON_ERR(a0_transport_empty(lk, &empty));
  if (A0_UNLIKELY(empty)) {
    return EAGAIN;
  }

  lk.transport->_seq = state->seq_low;
  lk.transport->_off = state->off_head;
  return A0_OK;
}

errno_t a0_transport_jump_tail(a0_locked_transport_t lk) {
  a0_transport_state_t* state = a0_transport_working_page(lk);

  bool empty;
  A0_RETURN_ERR_ON_ERR(a0_transport_empty(lk, &empty));
  if (A0_UNLIKELY(empty)) {
    return EAGAIN;
  }

  lk.transport->_seq = state->seq_high;
  lk.transport->_off = state->off_tail;
  return A0_OK;
}

errno_t a0_transport_has_next(a0_locked_transport_t lk, bool* out) {
  bool empty;
  A0_RETURN_ERR_ON_ERR(a0_transport_empty(lk, &empty));

  a0_transport_state_t* state = a0_transport_working_page(lk);
  *out = !empty && (lk.transport->_seq < state->seq_high);
  return A0_OK;
}

errno_t a0_transport_next(a0_locked_transport_t lk) {
  a0_transport_state_t* state = a0_transport_working_page(lk);

  bool has_next;
  A0_RETURN_ERR_ON_ERR(a0_transport_has_next(lk, &has_next));
  if (A0_UNLIKELY(!has_next)) {
    return EAGAIN;
  }

  if (A0_UNLIKELY(lk.transport->_seq < state->seq_low)) {
    lk.transport->_seq = state->seq_low;
    lk.transport->_off = state->off_head;
    return A0_OK;
  }

  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk.transport->_arena.ptr;
  a0_transport_frame_hdr_t* curr_frame_hdr =
      (a0_transport_frame_hdr_t*)((uint8_t*)hdr + lk.transport->_off);
  lk.transport->_off = curr_frame_hdr->next_off;

  a0_transport_frame_hdr_t* next_frame_hdr =
      (a0_transport_frame_hdr_t*)((uint8_t*)hdr + lk.transport->_off);
  lk.transport->_seq = next_frame_hdr->seq;

  return A0_OK;
}

errno_t a0_transport_has_prev(a0_locked_transport_t lk, bool* out) {
  bool empty;
  A0_RETURN_ERR_ON_ERR(a0_transport_empty(lk, &empty));

  a0_transport_state_t* state = a0_transport_working_page(lk);
  *out = !empty && (lk.transport->_seq > state->seq_low);
  return A0_OK;
}

errno_t a0_transport_prev(a0_locked_transport_t lk) {
  bool has_prev;
  A0_RETURN_ERR_ON_ERR(a0_transport_has_prev(lk, &has_prev));
  if (A0_UNLIKELY(!has_prev)) {
    return EAGAIN;
  }

  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk.transport->_arena.ptr;
  a0_transport_frame_hdr_t* curr_frame_hdr =
      (a0_transport_frame_hdr_t*)((uint8_t*)hdr + lk.transport->_off);
  lk.transport->_off = curr_frame_hdr->prev_off;

  a0_transport_frame_hdr_t* prev_frame_hdr =
      (a0_transport_frame_hdr_t*)((uint8_t*)hdr + lk.transport->_off);
  lk.transport->_seq = prev_frame_hdr->seq;

  return A0_OK;
}

errno_t a0_transport_await(a0_locked_transport_t* lk,
                           errno_t (*pred)(a0_locked_transport_t, bool*)) {
  if (A0_UNLIKELY(lk->transport->_closing)) {
    return ESHUTDOWN;
  }

  bool sat = false;
  errno_t err = pred(*lk, &sat);
  if (err | sat) {
    return err;
  }

  lk->transport->_await_cnt++;

  while (A0_LIKELY(!lk->transport->_closing)) {
    err = pred(*lk, &sat);
    if (err | sat) {
      break;
    }
    a0_wait_for_notify(lk);
  }
  if (!err && lk->transport->_closing) {
    err = ESHUTDOWN;
  }

  lk->transport->_await_cnt--;
  a0_schedule_notify(*lk);

  return err;
}

errno_t a0_transport_frame(a0_locked_transport_t lk, a0_transport_frame_t* frame_out) {
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk.transport->_arena.ptr;
  a0_transport_state_t* state = a0_transport_working_page(lk);

  if (A0_UNLIKELY(lk.transport->_seq < state->seq_low)) {
    return ESPIPE;
  }

  a0_transport_frame_hdr_t* frame_hdr =
      (a0_transport_frame_hdr_t*)((uint8_t*)hdr + lk.transport->_off);

  frame_out->hdr = *frame_hdr;
  frame_out->data = (uint8_t*)frame_hdr + sizeof(a0_transport_frame_hdr_t);
  return A0_OK;
}

A0_STATIC_INLINE
transport_off_t a0_transport_frame_end(a0_transport_hdr_t* hdr, transport_off_t frame_off) {
  a0_transport_frame_hdr_t* frame_hdr = (a0_transport_frame_hdr_t*)((uint8_t*)hdr + frame_off);
  return frame_off + sizeof(a0_transport_frame_hdr_t) + frame_hdr->data_size;
}

A0_STATIC_INLINE
bool a0_transport_frame_intersects(transport_off_t frame1_start,
                                   size_t frame1_size,
                                   transport_off_t frame2_start,
                                   size_t frame2_size) {
  transport_off_t frame1_end = frame1_start + frame1_size;
  transport_off_t frame2_end = frame2_start + frame2_size;
  return (frame1_start < frame2_end) && (frame2_start < frame1_end);
}

A0_STATIC_INLINE
bool a0_transport_head_interval(a0_locked_transport_t lk,
                                a0_transport_state_t* state,
                                transport_off_t* head_off,
                                size_t* head_size) {
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk.transport->_arena.ptr;

  bool empty;
  a0_transport_empty(lk, &empty);
  if (A0_UNLIKELY(empty)) {
    return false;
  }

  *head_off = state->off_head;
  a0_transport_frame_hdr_t* head_hdr = (a0_transport_frame_hdr_t*)((uint8_t*)hdr + *head_off);
  *head_size = sizeof(a0_transport_frame_hdr_t) + head_hdr->data_size;
  return true;
}

A0_STATIC_INLINE
void a0_transport_remove_head(a0_locked_transport_t lk, a0_transport_state_t* state) {
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk.transport->_arena.ptr;

  a0_transport_frame_hdr_t* head_hdr = (a0_transport_frame_hdr_t*)((uint8_t*)hdr + state->off_head);

  if (A0_UNLIKELY(state->off_head == state->off_tail)) {
    state->off_head = 0;
    state->off_tail = 0;
    state->seq_low++;
  } else {
    head_hdr = (a0_transport_frame_hdr_t*)((uint8_t*)hdr + head_hdr->next_off);
    state->off_head = head_hdr->off;
    state->seq_low = head_hdr->seq;
    head_hdr->prev_off = 0;
    // The following SHOULD work.
    // It is faster and passes all the tests, but I still have a weird feeling about it.
    // TODO(lshamis): Consider this alternative:
    // state->off_head = head_hdr->next_off;
    // state->seq_low++;
  }
  a0_transport_commit(lk);
}

A0_STATIC_INLINE
errno_t a0_transport_find_slot(a0_locked_transport_t lk, size_t frame_size, transport_off_t* off) {
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk.transport->_arena.ptr;
  a0_transport_state_t* state = a0_transport_working_page(lk);

  bool empty;
  A0_RETURN_ERR_ON_ERR(a0_transport_empty(lk, &empty));

  if (A0_UNLIKELY(empty)) {
    *off = a0_transport_workspace_off(hdr);
  } else {
    *off = a0_max_align(a0_transport_frame_end(hdr, state->off_tail));
    if (A0_UNLIKELY(*off + frame_size >= hdr->arena_size)) {
      *off = a0_transport_workspace_off(hdr);
    }
  }

  if (A0_UNLIKELY(*off + frame_size >= hdr->arena_size)) {
    return EOVERFLOW;
  }

  return A0_OK;
}

A0_STATIC_INLINE
void a0_transport_evict(a0_locked_transport_t lk, transport_off_t off, size_t frame_size) {
  transport_off_t head_off;
  size_t head_size;
  a0_transport_state_t* state = a0_transport_working_page(lk);
  while (a0_transport_head_interval(lk, state, &head_off, &head_size) &&
         a0_transport_frame_intersects(off, frame_size, head_off, head_size)) {
    a0_transport_remove_head(lk, state);
  }
}

A0_STATIC_INLINE
void a0_transport_slot_init(a0_transport_state_t* state,
                            a0_transport_frame_hdr_t* frame_hdr,
                            transport_off_t off,
                            size_t size) {
  memset(frame_hdr, 0, sizeof(a0_transport_frame_hdr_t));

  frame_hdr->seq = ++state->seq_high;
  if (A0_UNLIKELY(!state->seq_low)) {
    state->seq_low = frame_hdr->seq;
  }

  frame_hdr->off = off;
  frame_hdr->next_off = 0;

  frame_hdr->data_size = size;
}

A0_STATIC_INLINE
void a0_transport_maybe_set_head(a0_transport_state_t* state, a0_transport_frame_hdr_t* frame_hdr) {
  if (A0_UNLIKELY(!state->off_head)) {
    state->off_head = frame_hdr->off;
  }
}

A0_STATIC_INLINE
void a0_transport_update_tail(a0_transport_hdr_t* hdr,
                              a0_transport_state_t* state,
                              a0_transport_frame_hdr_t* frame_hdr) {
  if (A0_LIKELY(state->off_tail)) {
    a0_transport_frame_hdr_t* tail_frame_hdr =
        (a0_transport_frame_hdr_t*)((uint8_t*)hdr + state->off_tail);
    tail_frame_hdr->next_off = frame_hdr->off;
    frame_hdr->prev_off = state->off_tail;
  }
  state->off_tail = frame_hdr->off;
}

errno_t a0_transport_alloc_evicts(a0_locked_transport_t lk, size_t size, bool* out) {
  size_t frame_size = sizeof(a0_transport_frame_hdr_t) + size;

  transport_off_t off;
  A0_RETURN_ERR_ON_ERR(a0_transport_find_slot(lk, frame_size, &off));

  transport_off_t head_off;
  size_t head_size;
  a0_transport_state_t* state = a0_transport_working_page(lk);
  *out = a0_transport_head_interval(lk, state, &head_off, &head_size) &&
         a0_transport_frame_intersects(off, frame_size, head_off, head_size);

  return A0_OK;
}

errno_t a0_transport_alloc(a0_locked_transport_t lk, size_t size, a0_transport_frame_t* frame_out) {
  size_t frame_size = sizeof(a0_transport_frame_hdr_t) + size;

  transport_off_t off;
  A0_RETURN_ERR_ON_ERR(a0_transport_find_slot(lk, frame_size, &off));

  a0_transport_evict(lk, off, frame_size);

  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk.transport->_arena.ptr;
  // Note: a0_transport_evict commits changes, which invalidates state.
  //       Must grab state afterwards.
  a0_transport_state_t* state = a0_transport_working_page(lk);

  a0_transport_frame_hdr_t* frame_hdr = (a0_transport_frame_hdr_t*)((uint8_t*)hdr + off);
  a0_transport_slot_init(state, frame_hdr, off, size);

  a0_transport_maybe_set_head(state, frame_hdr);
  a0_transport_update_tail(hdr, state, frame_hdr);

  frame_out->hdr = *frame_hdr;
  frame_out->data = (uint8_t*)hdr + off + sizeof(a0_transport_frame_hdr_t);

  return A0_OK;
}

A0_STATIC_INLINE
errno_t a0_transport_alloc_impl(void* user_data, size_t size, a0_buf_t* buf_out) {
  a0_transport_frame_t frame;
  A0_RETURN_ERR_ON_ERR(a0_transport_alloc(*(a0_locked_transport_t*)user_data, size, &frame));
  buf_out->ptr = frame.data;
  buf_out->size = frame.hdr.data_size;
  return A0_OK;
}

errno_t a0_transport_allocator(a0_locked_transport_t* lk, a0_alloc_t* alloc_out) {
  alloc_out->user_data = lk;
  alloc_out->alloc = a0_transport_alloc_impl;
  alloc_out->dealloc = NULL;
  return A0_OK;
}

errno_t a0_transport_commit(a0_locked_transport_t lk) {
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk.transport->_arena.ptr;
  // Assume page A was the previously committed page and page B is the working
  // page that is ready to be committed. Both represent a valid state for the
  // transport. It's possible that the copying of B into A will fail (prog crash),
  // leaving A in an inconsistent state. We set B as the committed page, before
  // copying the page info.
  hdr->committed_page_idx = !hdr->committed_page_idx;
  *a0_transport_working_page(lk) = *a0_transport_committed_page(lk);

  a0_schedule_notify(lk);

  return A0_OK;
}

A0_STATIC_INLINE
void write_limited(FILE* f, a0_buf_t str) {
  size_t line_size = str.size;
  bool overflow = false;
  if (line_size > 32) {
    line_size = 29;
    overflow = true;
  }
  fwrite(str.ptr, sizeof(char), line_size, f);
  if (overflow) {
    fprintf(f, "...");
  }
}

void a0_transport_debugstr(a0_locked_transport_t lk, a0_buf_t* out) {
  a0_transport_hdr_t* hdr = (a0_transport_hdr_t*)lk.transport->_arena.ptr;

  a0_transport_state_t* committed_state = a0_transport_committed_page(lk);
  a0_transport_state_t* working_state = a0_transport_working_page(lk);

  FILE* ss = open_memstream((char**)&out->ptr, &out->size);
  // clang-format off
  fprintf(ss, "\n{\n");
  fprintf(ss, "  \"header\": {\n");
  fprintf(ss, "    \"arena_size\": %lu,\n", hdr->arena_size);
  fprintf(ss, "    \"committed_state\": {\n");
  fprintf(ss, "      \"seq_low\": %lu,\n", committed_state->seq_low);
  fprintf(ss, "      \"seq_high\": %lu,\n", committed_state->seq_high);
  fprintf(ss, "      \"off_head\": %lu,\n", committed_state->off_head);
  fprintf(ss, "      \"off_tail\": %lu\n", committed_state->off_tail);
  fprintf(ss, "    },\n");
  fprintf(ss, "    \"working_state\": {\n");
  fprintf(ss, "      \"seq_low\": %lu,\n", working_state->seq_low);
  fprintf(ss, "      \"seq_high\": %lu,\n", working_state->seq_high);
  fprintf(ss, "      \"off_head\": %lu,\n", working_state->off_head);
  fprintf(ss, "      \"off_tail\": %lu\n", working_state->off_tail);
  fprintf(ss, "    }\n");
  fprintf(ss, "  },\n");
  a0_buf_t metadata;
  a0_transport_metadata(lk, &metadata);
  fprintf(ss, "  \"metadata\": \"");  write_limited(ss, metadata); fprintf(ss, "\",\n");
  fprintf(ss, "  \"data\": [\n");
  // clang-format on

  if (working_state->off_head) {
    uint64_t off = working_state->off_head;
    bool first = true;
    while (true) {
      a0_transport_frame_hdr_t* frame_hdr = (a0_transport_frame_hdr_t*)((uint8_t*)hdr + off);
      uint64_t seq = frame_hdr->seq;

      if (!first) {
        fprintf(ss, "    },\n");
      }
      first = false;

      fprintf(ss, "    {\n");
      if (seq > committed_state->seq_high) {
        fprintf(ss, "      \"committed\": false,\n");
      }
      fprintf(ss, "      \"off\": %lu,\n", frame_hdr->off);
      fprintf(ss, "      \"seq\": %lu,\n", frame_hdr->seq);
      fprintf(ss, "      \"prev_off\": %lu,\n", frame_hdr->prev_off);
      fprintf(ss, "      \"next_off\": %lu,\n", frame_hdr->next_off);
      fprintf(ss, "      \"data_size\": %lu,\n", frame_hdr->data_size);
      a0_buf_t data = {
          .ptr = (uint8_t*)hdr + frame_hdr->off + sizeof(a0_transport_frame_hdr_t),
          .size = frame_hdr->data_size,
      };
      fprintf(ss, "      \"data\": \"");
      write_limited(ss, data);
      fprintf(ss, "\"\n");

      off = frame_hdr->next_off;

      if (seq == working_state->seq_high) {
        fprintf(ss, "    }\n");
        break;
      }
    }
  }
  fprintf(ss, "  ]\n");
  fprintf(ss, "}\n");
  fflush(ss);
  fclose(ss);
}
