#include <a0/alloc.h>
#include <a0/arena.h>
#include <a0/common.h>
#include <a0/errno.h>
#include <a0/packet.h>
#include <a0/pubsub.h>
#include <a0/time.h>
#include <a0/transport.h>
#include <a0/uuid.h>

#include <fcntl.h>

#include <cerrno>
#include <cstdint>
#include <ctime>
#include <memory>
#include <string_view>
#include <vector>

#include "alloc_util.hpp"
#include "charconv.hpp"
#include "macros.h"
#include "scope.hpp"
#include "sync.hpp"
#include "transport_tools.hpp"

#ifdef DEBUG
#include "ref_cnt.h"
#endif

// Use MONOTONIC_COARSE to avoid syscall cost at the expense of reduced resolution.
// On our systems, the coarse clock has a 4ms resolution, which is still good enough
// for >200Hz timings.
#define A0_PERF_CLOCK_SOURCE CLOCK_MONOTONIC_COARSE
// Warn for any allocations taking more than 5ms
#define A0_ALLOC_WARN_NS 5000000
// Warn for any transport manipulations taking more than 20ms
#define A0_MANIP_WARN_NS 20000000
// Warn for any transport packet handlers taking more than 100ms
#define A0_HANDLER_WARN_NS 100000000

namespace {

struct a0_pubsub_metadata_t {
  uint64_t transport_seq;
};

// Given a start time and end time, check to see if the timedelta is > the given
// threshold (a number of nanoseconds).
// If so:
//  * Emit a warning message on stderr with the @p preamble and
//    the measured and threshold timings.
// If not:
//  * Do nothing and return.
A0_STATIC_INLINE
void a0_warn_if_past_threshold(struct timespec* start, struct timespec* end, int64_t threshold_ns,
                               const char* preamble) {
  int64_t seconds = end->tv_sec - start->tv_sec;
  int64_t nanoseconds = (seconds * 1000000000) + (end->tv_nsec - start->tv_nsec);

  if (A0_UNLIKELY(nanoseconds > threshold_ns) && (threshold_ns != 0)) {
    const double kMsPerNs = 1e-6;
    fprintf(stderr, "Warning: %s %.3fms. Above threshold of %.3fms\n", preamble, nanoseconds * kMsPerNs, threshold_ns * kMsPerNs);
  }
}

};  // namespace

/////////////////
//  Publisher  //
/////////////////

struct a0_publisher_raw_impl_s {
  a0_transport_t transport;
};

errno_t a0_publisher_raw_init(a0_publisher_raw_t* pub, a0_arena_t arena) {
  auto impl = std::make_unique<a0_publisher_raw_impl_t>();

  a0_transport_init_status_t init_status;
  a0_locked_transport_t tlk;
  A0_RETURN_ERR_ON_ERR(a0_transport_init(&impl->transport, arena, &init_status, &tlk));
  bool empty;
  A0_RETURN_ERR_ON_ERR(a0_transport_empty(tlk, &empty));
  if (empty) {
    A0_RETURN_ERR_ON_ERR(a0_transport_init_metadata(tlk, sizeof(a0_pubsub_metadata_t)));
  }
  a0_transport_unlock(&tlk);

#ifdef DEBUG
  a0_ref_cnt_inc(arena.ptr);
#endif

  pub->_impl = impl.release();

  return A0_OK;
}

errno_t a0_publisher_raw_close(a0_publisher_raw_t* pub) {
  if (!pub || !pub->_impl) {
    return ESHUTDOWN;
  }

#ifdef DEBUG
  a0_ref_cnt_dec(pub->_impl->transport._arena.ptr);
#endif

  a0_transport_close(&pub->_impl->transport);
  delete pub->_impl;
  pub->_impl = nullptr;

  return A0_OK;
}

errno_t a0_pub_raw(a0_publisher_raw_t* pub, const a0_packet_t pkt) {
  if (!pub || !pub->_impl) {
    return ESHUTDOWN;
  }

  a0::scoped_transport_lock stlk(&pub->_impl->transport);
  a0_alloc_t alloc;
  A0_RETURN_ERR_ON_ERR(a0_transport_allocator(&stlk.tlk, &alloc));
  A0_RETURN_ERR_ON_ERR(a0_packet_serialize(pkt, alloc, nullptr));
  return a0_transport_commit(stlk.tlk);
}

static constexpr std::string_view TRANSPORT_SEQ = "a0_transport_seq";
static constexpr std::string_view PUBLISHER_SEQ = "a0_publisher_seq";
static constexpr std::string_view PUBLISHER_ID = "a0_publisher_id";

struct a0_publisher_impl_s {
  a0_publisher_raw_t raw;
  uint64_t publisher_seq{0};
  a0_uuid_t id;
};

errno_t a0_publisher_init(a0_publisher_t* pub, a0_arena_t arena) {
  pub->_impl = new a0_publisher_impl_t;
  a0_uuidv4(pub->_impl->id);
  return a0_publisher_raw_init(&pub->_impl->raw, arena);
}

errno_t a0_publisher_close(a0_publisher_t* pub) {
  if (!pub || !pub->_impl) {
    return ESHUTDOWN;
  }

  a0_publisher_raw_close(&pub->_impl->raw);
  delete pub->_impl;
  pub->_impl = nullptr;

  return A0_OK;
}

errno_t a0_pub(a0_publisher_t* pub, const a0_packet_t pkt) {
  if (!pub || !pub->_impl) {
    return ESHUTDOWN;
  }

  uint64_t time_mono;
  a0_time_mono_now(&time_mono);
  char mono_str[20];
  a0_time_mono_str(time_mono, mono_str);

  timespec time_wall;
  a0_time_wall_now(&time_wall);
  char wall_str[36];
  a0_time_wall_str(time_wall, wall_str);

  char pseq_str[20];
  a0::to_chars(pseq_str, pseq_str + 20, pub->_impl->publisher_seq++);

  char tseq_str[20];
  {
    a0::scoped_transport_lock stlk(&pub->_impl->raw._impl->transport);
    a0_buf_t metadata;
    a0_transport_metadata(stlk.tlk, &metadata);
    a0::to_chars(tseq_str, tseq_str + 20, ((a0_pubsub_metadata_t*)metadata.ptr)->transport_seq++);
  }

  constexpr size_t num_extra_headers = 5;
  a0_packet_header_t extra_headers[num_extra_headers] = {
      {A0_TIME_MONO, mono_str},
      {A0_TIME_WALL, wall_str},
      {TRANSPORT_SEQ.data(), tseq_str},
      {PUBLISHER_SEQ.data(), pseq_str},
      {PUBLISHER_ID.data(), pub->_impl->id},
  };

  a0_packet_t full_pkt = pkt;
  full_pkt.headers_block = (a0_packet_headers_block_t){
      .headers = extra_headers,
      .size = num_extra_headers,
      .next_block = (a0_packet_headers_block_t*)&pkt.headers_block,
  };

  return a0_pub_raw(&pub->_impl->raw, full_pkt);
}

//////////////////
//  Subscriber  //
//////////////////

// Synchronous zero-copy version.

struct a0_subscriber_sync_zc_impl_s {
  a0_transport_t transport;

  a0_subscriber_init_t sub_init;
  a0_subscriber_iter_t sub_iter;

  bool read_first{false};
};

errno_t a0_subscriber_sync_zc_init(a0_subscriber_sync_zc_t* sub_sync_zc,
                                   a0_arena_t arena,
                                   a0_subscriber_init_t sub_init,
                                   a0_subscriber_iter_t sub_iter) {
  sub_sync_zc->_impl = new a0_subscriber_sync_zc_impl_t;
  sub_sync_zc->_impl->sub_init = sub_init;
  sub_sync_zc->_impl->sub_iter = sub_iter;

  a0_transport_init_status_t init_status;
  a0_locked_transport_t tlk;
  a0_transport_init(&sub_sync_zc->_impl->transport,
                    arena,
                    &init_status,
                    &tlk);
  a0_transport_unlock(&tlk);

#ifdef DEBUG
  a0_ref_cnt_inc(arena.ptr);
#endif

  return A0_OK;
}

errno_t a0_subscriber_sync_zc_close(a0_subscriber_sync_zc_t* sub_sync_zc) {
  if (!sub_sync_zc || !sub_sync_zc->_impl) {
    return ESHUTDOWN;
  }

#ifdef DEBUG
  a0_ref_cnt_dec(sub_sync_zc->_impl->transport._arena.ptr);
#endif

  a0_transport_close(&sub_sync_zc->_impl->transport);
  delete sub_sync_zc->_impl;
  sub_sync_zc->_impl = nullptr;

  return A0_OK;
}

errno_t a0_subscriber_sync_zc_has_next(a0_subscriber_sync_zc_t* sub_sync_zc, bool* has_next) {
  if (!sub_sync_zc || !sub_sync_zc->_impl) {
    return ESHUTDOWN;
  }

  a0::scoped_transport_lock stlk(&sub_sync_zc->_impl->transport);
  return a0_transport_has_next(stlk.tlk, has_next);
}

errno_t a0_subscriber_sync_zc_next(a0_subscriber_sync_zc_t* sub_sync_zc,
                                   a0_zero_copy_callback_t cb) {
  if (!sub_sync_zc || !sub_sync_zc->_impl) {
    return ESHUTDOWN;
  }

  a0::scoped_transport_lock stlk(&sub_sync_zc->_impl->transport);

  if (!sub_sync_zc->_impl->read_first) {
    if (sub_sync_zc->_impl->sub_init == A0_INIT_OLDEST) {
      a0_transport_jump_head(stlk.tlk);
    } else if (sub_sync_zc->_impl->sub_init == A0_INIT_MOST_RECENT ||
               sub_sync_zc->_impl->sub_init == A0_INIT_AWAIT_NEW) {
      a0_transport_jump_tail(stlk.tlk);
    }
  } else {
    if (sub_sync_zc->_impl->sub_iter == A0_ITER_NEXT) {
      a0_transport_next(stlk.tlk);
    } else if (sub_sync_zc->_impl->sub_iter == A0_ITER_NEWEST) {
      a0_transport_jump_tail(stlk.tlk);
    }
  }

  a0_transport_frame_t frame;
  a0_transport_frame(stlk.tlk, &frame);

  thread_local a0::scope<a0_alloc_t> headers_alloc = a0::scope_realloc();

  a0_packet_t pkt;
  a0_packet_deserialize(a0::buf(frame), *headers_alloc, &pkt);

  cb.fn(cb.user_data, &(stlk.tlk), pkt);
  sub_sync_zc->_impl->read_first = true;

  return A0_OK;
}

// Synchronous allocated version.

struct a0_subscriber_sync_impl_s {
  a0_subscriber_sync_zc_t sub_sync_zc;

  a0_alloc_t alloc;
};

errno_t a0_subscriber_sync_init(a0_subscriber_sync_t* sub_sync,
                                a0_arena_t arena,
                                a0_alloc_t alloc,
                                a0_subscriber_init_t sub_init,
                                a0_subscriber_iter_t sub_iter) {
  sub_sync->_impl = new a0_subscriber_sync_impl_t;

  sub_sync->_impl->alloc = alloc;
  return a0_subscriber_sync_zc_init(&sub_sync->_impl->sub_sync_zc, arena, sub_init, sub_iter);
}

errno_t a0_subscriber_sync_close(a0_subscriber_sync_t* sub_sync) {
  if (!sub_sync || !sub_sync->_impl) {
    return ESHUTDOWN;
  }

  a0_subscriber_sync_zc_close(&sub_sync->_impl->sub_sync_zc);
  delete sub_sync->_impl;
  sub_sync->_impl = nullptr;

  return A0_OK;
}

errno_t a0_subscriber_sync_has_next(a0_subscriber_sync_t* sub_sync, bool* has_next) {
  if (!sub_sync || !sub_sync->_impl) {
    return ESHUTDOWN;
  }

  return a0_subscriber_sync_zc_has_next(&sub_sync->_impl->sub_sync_zc, has_next);
}

errno_t a0_subscriber_sync_next(a0_subscriber_sync_t* sub_sync, a0_packet_t* pkt) {
  if (!sub_sync || !sub_sync->_impl) {
    return ESHUTDOWN;
  }

  struct data_t {
    a0_alloc_t alloc;
    a0_packet_t* pkt;
  } data{sub_sync->_impl->alloc, pkt};

  a0_zero_copy_callback_t wrapped_cb = {
      .user_data = &data,
      .fn =
          [](void* user_data, a0_locked_transport_t* tlk, a0_packet_t /*pkt_zc*/) {
            auto* data = (data_t*)user_data;
            // While holding the transport lock, copy the raw serialized frame
            // bytes to a thread-local buffer (fast, bounded memcpy). Then release
            // the lock before allocating. Calling data->alloc (typically malloc)
            // under the lock causes every other subscriber and publisher to stall
            // for the full duration of the allocation when the allocator is slow
            // (heap contention, page faults, fragmentation, etc.).
            a0_transport_frame_t raw_frame;
            a0_transport_frame(*tlk, &raw_frame);
            thread_local std::vector<uint8_t> local_frame_bytes;
            local_frame_bytes.resize(raw_frame.hdr.data_size);
            memcpy(local_frame_bytes.data(), raw_frame.data, raw_frame.hdr.data_size);
            // Release the transport lock. scoped_transport_unlock re-acquires on
            // destruction, so the parent (a0_subscriber_sync_zc_next) still sees
            // the lock held when we return and unlocks cleanly.
            a0::scoped_transport_unlock stulk(tlk);

            struct timespec start_copy;
            struct timespec end_copy;
            A0_ASSERT_OK(clock_gettime(A0_PERF_CLOCK_SOURCE, &start_copy), "Failed clock_gettime");
            a0_buf_t local_buf = {local_frame_bytes.data(), local_frame_bytes.size()};
            thread_local a0::scope<a0_alloc_t> local_headers_alloc = a0::scope_realloc();
            a0_packet_t pkt_local;
            a0_packet_deserialize(local_buf, *local_headers_alloc, &pkt_local);
            a0_packet_deep_copy(pkt_local, data->alloc, data->pkt);
            A0_ASSERT_OK(clock_gettime(A0_PERF_CLOCK_SOURCE, &end_copy), "Failed clock_gettime");
            a0_warn_if_past_threshold(&start_copy, &end_copy, A0_ALLOC_WARN_NS, "sub_sync_next allocation");
          },
  };
  return a0_subscriber_sync_zc_next(&sub_sync->_impl->sub_sync_zc, wrapped_cb);
}

// Zero-copy threaded version.

struct a0_subscriber_zc_impl_s {
  a0::transport_thread worker;
  bool started_empty;
};

errno_t a0_subscriber_zc_init(a0_subscriber_zc_t* sub_zc,
                              a0_arena_t arena,
                              a0_subscriber_init_t sub_init,
                              a0_subscriber_iter_t sub_iter,
                              a0_zero_copy_callback_t onmsg) {
  sub_zc->_impl = new a0_subscriber_zc_impl_t;

  auto on_transport_init = [sub_zc, sub_init](a0_locked_transport_t tlk,
                                              a0_transport_init_status_t) -> errno_t {
    // TODO(lshamis): Validate transport.

    a0_transport_empty(tlk, &sub_zc->_impl->started_empty);
    if (!sub_zc->_impl->started_empty) {
      if (sub_init == A0_INIT_OLDEST) {
        a0_transport_jump_head(tlk);
      } else if (sub_init == A0_INIT_MOST_RECENT || sub_init == A0_INIT_AWAIT_NEW) {
        a0_transport_jump_tail(tlk);
      }
    }

    return A0_OK;
  };

  auto handle_pkt = [onmsg](a0_locked_transport_t* tlk) {
    // Read the frame pointer while holding the transport lock.  frame.data is
    // a pointer into the arena (persistent shared memory); frame.hdr contains
    // the frame header fields including offset and data_size.
    a0_transport_frame_t frame;
    a0_transport_frame(*tlk, &frame);

    // Release the transport lock immediately after obtaining the frame
    // pointer.  This allows publishers and other subscribers to proceed
    // without waiting for the memcpy or the user callback.  We use a seqlock
    // to detect if a concurrent commit overwrites this frame's arena slot
    // while we are copying.
    a0_transport_unlock(tlk);

    thread_local std::vector<uint8_t> frame_bytes;
    uint32_t seq;
    do {
      seq = a0_transport_seqcount(tlk->transport);
      // Re-read data_size each iteration: if this slot was evicted and reused
      // by a newer frame, data_size may have changed.  The seqlock end-check
      // validates that both data_size and the copied bytes are consistent.
      const auto* fhdr = reinterpret_cast<const a0_transport_frame_hdr_t*>(
          static_cast<const uint8_t*>(tlk->transport->_arena.ptr) + frame.hdr.off);
      frame_bytes.resize(static_cast<size_t>(fhdr->data_size));
      memcpy(frame_bytes.data(), frame.data, frame_bytes.size());
    } while (!a0_transport_seqcount_valid(tlk->transport, seq));

    // Deserialize entirely outside the lock.
    thread_local a0::scope<a0_alloc_t> headers_alloc = a0::scope_realloc();
    a0_buf_t local_buf = {frame_bytes.data(), frame_bytes.size()};
    a0_packet_t pkt;
    a0_packet_deserialize(local_buf, *headers_alloc, &pkt);

    // Re-acquire the transport lock before invoking the zero-copy callback:
    // the a0_zero_copy_callback_t contract requires a valid locked transport.
    a0_transport_lock(tlk->transport, tlk);
    onmsg.fn(onmsg.user_data, tlk, pkt);
  };

  auto on_transport_nonempty = [sub_zc, sub_init, handle_pkt](a0_locked_transport_t* tlk) {
    bool reset = false;
    if (sub_zc->_impl->started_empty) {
      reset = true;
    } else {
      bool ptr_valid;
      a0_transport_ptr_valid(*tlk, &ptr_valid);
      reset = !ptr_valid;
    }

    if (reset) {
      a0_transport_jump_head(*tlk);
    }

    if (reset || sub_init == A0_INIT_OLDEST || sub_init == A0_INIT_MOST_RECENT) {
      handle_pkt(tlk);
    }
  };

  auto on_transport_hasnext = [sub_iter, handle_pkt](a0_locked_transport_t* tlk) {
    struct timespec start_transport_manip;
    struct timespec end_transport_manip;
    struct timespec end_handler;
    A0_ASSERT_OK(clock_gettime(A0_PERF_CLOCK_SOURCE, &start_transport_manip), "Failed clock_gettime");
    if (sub_iter == A0_ITER_NEXT) {
      a0_transport_next(*tlk);
    } else if (sub_iter == A0_ITER_NEWEST) {
      a0_transport_jump_tail(*tlk);
    }
    A0_ASSERT_OK(clock_gettime(A0_PERF_CLOCK_SOURCE, &end_transport_manip), "Failed clock_gettime");
    a0_warn_if_past_threshold(&start_transport_manip, &end_transport_manip, A0_MANIP_WARN_NS, "transport_hasnext manip");

    handle_pkt(tlk);
    A0_ASSERT_OK(clock_gettime(A0_PERF_CLOCK_SOURCE, &end_handler), "Failed clock_gettime");
    a0_warn_if_past_threshold(&end_transport_manip, &end_handler, A0_HANDLER_WARN_NS, "transport_hasnext handler");
  };

  return sub_zc->_impl->worker.init(arena,
                                    on_transport_init,
                                    on_transport_nonempty,
                                    on_transport_hasnext);
}

errno_t a0_subscriber_zc_async_close(a0_subscriber_zc_t* sub_zc, a0_callback_t onclose) {
  if (!sub_zc || !sub_zc->_impl) {
    return ESHUTDOWN;
  }

  sub_zc->_impl->worker.async_close([sub_zc, onclose]() {
    delete sub_zc->_impl;
    sub_zc->_impl = nullptr;

    if (onclose.fn) {
      onclose.fn(onclose.user_data);
    }
  });

  return A0_OK;
}

errno_t a0_subscriber_zc_close(a0_subscriber_zc_t* sub_zc) {
  if (!sub_zc || !sub_zc->_impl) {
    return ESHUTDOWN;
  }

  sub_zc->_impl->worker.await_close();
  delete sub_zc->_impl;
  sub_zc->_impl = nullptr;

  return A0_OK;
}

// Normal threaded version.

struct a0_subscriber_impl_s {
  // Worker thread is owned directly rather than via a0_subscriber_zc_t.
  // This allows handle_pkt to use a two-phase lock protocol (one acquire/
  // release pair per message) instead of the three pairs required when going
  // through the ZC callback API.
  a0::transport_thread worker;
  bool started_empty;

  a0_alloc_t alloc;
  a0_packet_callback_t onmsg;
};

// Frame data smaller than this threshold is copied while the transport lock is
// held.  Larger frames use a seqlock-protected copy (unlock → copy → re-acquire)
// so the lock hold is bounded regardless of message size.
//
// Rationale: one PI-futex round-trip costs ~2 µs uncontended.  At ~10 GB/s
// memcpy bandwidth, a 16 kB frame takes ~1.5 µs to copy.  Below ~16 kB the
// extra lock round-trip is not worth the parallelism it buys.
static constexpr size_t A0_SUB_SEQLOCK_COPY_THRESHOLD = 16 * 1024;  // 16 kB

errno_t a0_subscriber_init(a0_subscriber_t* sub,
                           a0_arena_t arena,
                           a0_alloc_t alloc,
                           a0_subscriber_init_t sub_init,
                           a0_subscriber_iter_t sub_iter,
                           a0_packet_callback_t onmsg) {
  sub->_impl = new a0_subscriber_impl_t;
  sub->_impl->alloc = alloc;
  sub->_impl->onmsg = onmsg;

  // Optimised packet handler for the non-ZC subscriber path.
  //
  // Lock protocol per message (2 acquires, 2 releases):
  //   [caller holds lock]
  //   Small frame: advance + memcpy under lock → unlock once → deep_copy +
  //       user_callback outside lock → re-acquire before returning.
  //   Large frame: advance under lock → unlock → seqlock memcpy → deep_copy +
  //       user_callback outside lock → re-acquire before returning.
  //
  // This eliminates the unnecessary lock pair present in the ZC path, where
  // handle_pkt re-acquires after the seqlock copy and wrapped_onmsg
  // immediately releases again.
  auto handle_pkt = [impl = sub->_impl](a0_locked_transport_t* tlk) {
    a0_transport_frame_t frame;
    a0_transport_frame(*tlk, &frame);

    thread_local std::vector<uint8_t> frame_bytes;
    if (frame.hdr.data_size <= A0_SUB_SEQLOCK_COPY_THRESHOLD) {
      // Small frame: copy while holding the lock.  The hold is short and
      // bounded (nanoseconds for typical small messages); no seqlock needed.
      frame_bytes.resize(static_cast<size_t>(frame.hdr.data_size));
      memcpy(frame_bytes.data(), frame.data, frame_bytes.size());
      a0_transport_unlock(tlk);
    } else {
      // Large frame: release the lock before the potentially expensive copy
      // so concurrent publishers and other subscribers are not blocked.
      // Use a seqlock to detect if a concurrent commit evicted / overwrote
      // this arena slot during the copy and retry if so.
      a0_transport_unlock(tlk);
      uint32_t seq;
      do {
        seq = a0_transport_seqcount(tlk->transport);
        const auto* fhdr = reinterpret_cast<const a0_transport_frame_hdr_t*>(
            static_cast<const uint8_t*>(tlk->transport->_arena.ptr) + frame.hdr.off);
        frame_bytes.resize(static_cast<size_t>(fhdr->data_size));
        memcpy(frame_bytes.data(), frame.data, frame_bytes.size());
      } while (!a0_transport_seqcount_valid(tlk->transport, seq));
    }

    // Deserialize, deep_copy, and invoke the user callback — all outside the
    // transport lock.  impl->alloc may call malloc; holding hdr->mu across
    // that stalls every other subscriber and publisher.
    thread_local a0::scope<a0_alloc_t> headers_alloc = a0::scope_realloc();
    a0_buf_t local_buf = {frame_bytes.data(), frame_bytes.size()};
    a0_packet_t pkt_deserialized;
    a0_packet_deserialize(local_buf, *headers_alloc, &pkt_deserialized);

    struct timespec start_copy;
    struct timespec end_copy;
    A0_ASSERT_OK(clock_gettime(A0_PERF_CLOCK_SOURCE, &start_copy), "Failed clock_gettime");
    a0_packet_t pkt;
    a0_packet_deep_copy(pkt_deserialized, impl->alloc, &pkt);
    A0_ASSERT_OK(clock_gettime(A0_PERF_CLOCK_SOURCE, &end_copy), "Failed clock_gettime");
    a0_warn_if_past_threshold(&start_copy, &end_copy, A0_ALLOC_WARN_NS, "sub_callback allocation");

    impl->onmsg.fn(impl->onmsg.user_data, pkt);

    // Re-acquire the transport lock before returning.  The caller
    // (transport_thread::handle_next_pkt / handle_first_pkt) holds a
    // scoped_transport_lock that will call a0_transport_unlock() when it
    // goes out of scope; the lock must be held at that point.
    a0_transport_lock(tlk->transport, tlk);
  };

  auto on_transport_init = [impl = sub->_impl, sub_init](
                               a0_locked_transport_t tlk,
                               a0_transport_init_status_t) -> errno_t {
    a0_transport_empty(tlk, &impl->started_empty);
    if (!impl->started_empty) {
      if (sub_init == A0_INIT_OLDEST) {
        a0_transport_jump_head(tlk);
      } else if (sub_init == A0_INIT_MOST_RECENT || sub_init == A0_INIT_AWAIT_NEW) {
        a0_transport_jump_tail(tlk);
      }
    }
    return A0_OK;
  };

  auto on_transport_nonempty = [impl = sub->_impl, sub_init, handle_pkt](
                                   a0_locked_transport_t* tlk) {
    bool reset = false;
    if (impl->started_empty) {
      reset = true;
    } else {
      bool ptr_valid;
      a0_transport_ptr_valid(*tlk, &ptr_valid);
      reset = !ptr_valid;
    }

    if (reset) {
      a0_transport_jump_head(*tlk);
    }

    if (reset || sub_init == A0_INIT_OLDEST || sub_init == A0_INIT_MOST_RECENT) {
      handle_pkt(tlk);
    }
  };

  auto on_transport_hasnext = [sub_iter, handle_pkt](a0_locked_transport_t* tlk) {
    struct timespec start_transport_manip;
    struct timespec end_transport_manip;
    struct timespec end_handler;
    A0_ASSERT_OK(clock_gettime(A0_PERF_CLOCK_SOURCE, &start_transport_manip), "Failed clock_gettime");
    if (sub_iter == A0_ITER_NEXT) {
      a0_transport_next(*tlk);
    } else if (sub_iter == A0_ITER_NEWEST) {
      a0_transport_jump_tail(*tlk);
    }
    A0_ASSERT_OK(clock_gettime(A0_PERF_CLOCK_SOURCE, &end_transport_manip), "Failed clock_gettime");
    a0_warn_if_past_threshold(&start_transport_manip, &end_transport_manip, A0_MANIP_WARN_NS, "transport_hasnext manip");

    handle_pkt(tlk);
    A0_ASSERT_OK(clock_gettime(A0_PERF_CLOCK_SOURCE, &end_handler), "Failed clock_gettime");
    a0_warn_if_past_threshold(&end_transport_manip, &end_handler, A0_HANDLER_WARN_NS, "transport_hasnext handler");
  };

  return sub->_impl->worker.init(arena,
                                 on_transport_init,
                                 on_transport_nonempty,
                                 on_transport_hasnext);
}

errno_t a0_subscriber_close(a0_subscriber_t* sub) {
  if (!sub || !sub->_impl) {
    return ESHUTDOWN;
  }

  auto err = sub->_impl->worker.await_close();
  delete sub->_impl;
  sub->_impl = nullptr;

  return err;
}

errno_t a0_subscriber_async_close(a0_subscriber_t* sub, a0_callback_t onclose) {
  if (!sub || !sub->_impl) {
    return ESHUTDOWN;
  }

  // clang-tidy thinks the lambda capture of sub->_impl is a leak.
  // It can't track it through the async callback.
  // NOLINTNEXTLINE(clang-analyzer-cplusplus.NewDeleteLeaks)
  return sub->_impl->worker.async_close([sub, onclose]() {
    delete sub->_impl;
    sub->_impl = nullptr;
    if (onclose.fn) {
      onclose.fn(onclose.user_data);
    }
  });
}

// One-off reader.

errno_t a0_subscriber_read_one(a0_arena_t arena,
                               a0_alloc_t alloc,
                               a0_subscriber_init_t sub_init,
                               int flags,
                               a0_packet_t* out) {
  if (flags & O_NDELAY || flags & O_NONBLOCK) {
    if (sub_init == A0_INIT_AWAIT_NEW) {
      return EAGAIN;
    }

    a0_subscriber_sync_t sub_sync;
    A0_RETURN_ERR_ON_ERR(a0_subscriber_sync_init(&sub_sync, arena, alloc, sub_init, A0_ITER_NEXT));
    struct sub_guard {
      a0_subscriber_sync_t* sub_sync;
      ~sub_guard() {
        a0_subscriber_sync_close(sub_sync);
      }
    } sub_guard_{&sub_sync};

    bool has_next;
    A0_RETURN_ERR_ON_ERR(a0_subscriber_sync_has_next(&sub_sync, &has_next));
    if (!has_next) {
      return EAGAIN;
    }
    A0_RETURN_ERR_ON_ERR(a0_subscriber_sync_next(&sub_sync, out));
  } else {
    struct data_ {
      a0_packet_t* pkt;

      a0::Event sub_event{};
      a0::Event done_event{};
    } data{.pkt = out};

    a0_packet_callback_t cb = {
        .user_data = &data,
        .fn =
            [](void* user_data, a0_packet_t pkt) {
              auto* data = (data_*)user_data;
              if (data->done_event.is_set()) {
                return;
              }

              data->sub_event.wait();
              *data->pkt = pkt;
              data->done_event.set();
            },
    };

    a0_subscriber_t sub;
    A0_RETURN_ERR_ON_ERR(a0_subscriber_init(&sub, arena, alloc, sub_init, A0_ITER_NEXT, cb));

    data.sub_event.set();
    data.done_event.wait();

    A0_RETURN_ERR_ON_ERR(a0_subscriber_close(&sub));
  }

  return A0_OK;
}
