// Message-age benchmark
//
// Measures how stale consumed messages are relative to the most recently
// published one — a proxy for subscriber lag when the publisher runs as
// fast as possible without waiting for subscribers.
//
// "Message age" at consumption time is defined as:
//
//     age = latest_pub_ts - msg_pub_ts
//
// where:
//   msg_pub_ts    = nanosecond timestamp embedded in the first 8 payload
//                   bytes by the publisher immediately before a0_pub.
//   latest_pub_ts = shared atomic updated by the publisher before each
//                   a0_pub; always reflects the most recently initiated
//                   publish at the moment the subscriber reads it.
//
// Subscribers use A0_ITER_NEWEST: on every wakeup the worker jumps to the
// ring-buffer tail before copying.  This means:
//   - Ring-buffer wraps are handled correctly — no stale-cursor hang.
//   - Subscribers naturally skip messages when the publisher is faster,
//     producing a non-zero age that grows with subscriber lag.
//
// Drain termination uses a sentinel message (payload prefix = UINT64_MAX)
// rather than a fixed callback count.  The sentinel is published last; with
// ITER_NEWEST every subscriber eventually reads it regardless of skipping.
//
// The simulated deserialization runs *before* sampling latest_pub_ns so any
// time spent in the callback is included in the reported age.
//
// Three sweeps are run:
//   A) Vary subscriber count  (fixed 1 kB payload, no deser)
//   B) Vary message size      (fixed 16 subscribers, no deser)
//   C) Vary deser cost        (fixed 16 subscribers, 1 kB payload)

#include <a0.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sched.h>
#include <string>
#include <vector>

static const char BENCH_SHM[] = "bench_pubsub_age.shm";

// Sentinel value stored in the first 8 payload bytes to signal end-of-burst.
// steady_clock nanoseconds since epoch is currently ~1.7e18; UINT64_MAX
// (~1.8e19) is safely distinguishable.
static constexpr uint64_t SENTINEL_TS = UINT64_MAX;

static inline uint64_t now_ns() {
  return static_cast<uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
}

// ── Statistics ───────────────────────────────────────────────────────────────

struct AgeStats {
  uint64_t sum_ns = 0;
  uint64_t max_ns = 0;
  uint64_t count  = 0;

  void record(uint64_t ns) {
    sum_ns += ns;
    if (ns > max_ns) max_ns = ns;
    ++count;
  }

  void merge(const AgeStats& o) {
    sum_ns += o.sum_ns;
    if (o.max_ns > max_ns) max_ns = o.max_ns;
    count += o.count;
  }

  double mean_us() const {
    return count ? static_cast<double>(sum_ns) / static_cast<double>(count) / 1e3 : 0.0;
  }
  double max_us() const { return static_cast<double>(max_ns) / 1e3; }
};

// ── Deserialization simulator ─────────────────────────────────────────────────

static void simulate_deser(a0_packet_t pkt, uint64_t deser_ns) {
  if (deser_ns == 0 || pkt.payload.size == 0) return;
  const uint8_t* data = pkt.payload.ptr;
  const size_t   len  = pkt.payload.size;
  volatile uint8_t sink = 0;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::nanoseconds(deser_ns);
  size_t i = 0;
  do {
    sink = static_cast<uint8_t>(sink + data[i % len]);
    ++i;
  } while (std::chrono::steady_clock::now() < deadline);
  (void)sink;
}

// ── Per-subscriber context ────────────────────────────────────────────────────

struct SubCtx {
  a0_alloc_t                   alloc;
  const std::atomic<uint64_t>* latest_pub_ns;
  uint64_t                     deser_ns;

  // Warmup: first non-sentinel message primes the subscriber thread; we
  // count it separately and reset stats after all subscribers have warmed up.
  std::atomic<int>* warmup_received;  // shared across all subscribers
  bool              warmed_up{false};

  // Per-subscriber tally (reset after warmup).
  AgeStats stats;
  uint64_t callback_count{0};  // data messages processed (excludes sentinel)

  // Set true when the sentinel message is received; used for drain.
  std::atomic<bool> done{false};
};

// ── Benchmark runner ──────────────────────────────────────────────────────────

static void run_bench(const char* name,
                      int         num_subs,
                      int         msg_size,
                      uint64_t    deser_ns,
                      int         num_msgs) {
  a0_file_remove(BENCH_SHM);
  a0_file_t file;
  a0_file_open(BENCH_SHM, nullptr, &file);

  // Publisher stores the timestamp of the most recently initiated publish
  // before each a0_pub call.  Subscribers read this after deserialization to
  // compute how far the publisher has moved on.
  std::atomic<uint64_t> latest_pub_ns{0};
  std::atomic<int> warmup_received{0};

  // ── Subscribers ──
  // A0_INIT_MOST_RECENT + A0_ITER_NEWEST: start at the current tail and
  // always jump to the newest available message on each wakeup.  This
  // naturally handles ring-buffer wraps without any subscriber-side logic.
  std::vector<SubCtx> ctxs(num_subs);
  std::vector<a0_subscriber_t> subs(num_subs);

  for (int i = 0; i < num_subs; i++) {
    a0_realloc_allocator_init(&ctxs[i].alloc);
    ctxs[i].latest_pub_ns   = &latest_pub_ns;
    ctxs[i].deser_ns        = deser_ns;
    ctxs[i].warmup_received = &warmup_received;

    a0_packet_callback_t cb = {
        .user_data = &ctxs[i],
        .fn =
            [](void* user_data, a0_packet_t pkt) {
              auto* ctx = static_cast<SubCtx*>(user_data);
              if (pkt.payload.size < sizeof(uint64_t)) return;

              uint64_t msg_ts;
              memcpy(&msg_ts, pkt.payload.ptr, sizeof(msg_ts));

              // Sentinel: signal drain and return without recording stats.
              if (msg_ts == SENTINEL_TS) {
                ctx->done.store(true, std::memory_order_release);
                return;
              }

              // First non-sentinel message is the warmup round.
              if (!ctx->warmed_up) {
                ctx->warmed_up = true;
                ctx->warmup_received->fetch_add(1, std::memory_order_release);
                return;
              }

              // Simulate deserialization before reading latest_pub_ns so that
              // callback processing time contributes to the reported age.
              simulate_deser(pkt, ctx->deser_ns);

              const uint64_t latest =
                  ctx->latest_pub_ns->load(std::memory_order_acquire);
              if (latest >= msg_ts) {
                ctx->stats.record(latest - msg_ts);
              }
              ctx->callback_count++;
            },
    };

    a0_subscriber_init(
        &subs[i], file.arena, ctxs[i].alloc, A0_INIT_MOST_RECENT, A0_ITER_NEWEST, cb);
  }

  // ── Publisher ──
  a0_publisher_t pub;
  a0_publisher_init(&pub, file.arena);

  // Payload: first 8 bytes = publish timestamp (or SENTINEL_TS); rest = pad.
  const int payload_size = std::max(msg_size, static_cast<int>(sizeof(uint64_t)));
  std::string payload(payload_size, 'x');
  a0_packet_t pkt;
  a0_packet_init(&pkt);
  pkt.payload = {.ptr = (uint8_t*)payload.data(), .size = (size_t)payload_size};

  // ── Warmup ──
  // Publish one message and wait for every subscriber thread to receive it.
  // This guarantees all worker threads are live and parked on the transport
  // before the timed loop starts (avoiding thread-startup latency in stats).
  {
    uint64_t ts = now_ns();
    memcpy(payload.data(), &ts, sizeof(ts));
    latest_pub_ns.store(ts, std::memory_order_release);
    a0_pub(&pub, pkt);
    while (warmup_received.load(std::memory_order_acquire) < num_subs) {
      sched_yield();
    }
  }

  // ── Timed publish loop ──
  // Publisher runs flat-out without waiting for subscribers, intentionally
  // racing ahead so that subscriber lag becomes visible in the age metric.
  for (int m = 0; m < num_msgs; m++) {
    uint64_t ts = now_ns();
    memcpy(payload.data(), &ts, sizeof(ts));
    latest_pub_ns.store(ts, std::memory_order_release);
    a0_pub(&pub, pkt);
  }

  // ── Sentinel ──
  // Signal end-of-burst.  Do NOT update latest_pub_ns to SENTINEL_TS so that
  // the last recorded data-message age remains meaningful.
  {
    uint64_t sent = SENTINEL_TS;
    memcpy(payload.data(), &sent, sizeof(sent));
    a0_pub(&pub, pkt);
  }

  // ── Drain ──
  // With A0_ITER_NEWEST the sentinel is always at the tail after publishing.
  // Every subscriber worker will eventually jump_tail → process sentinel →
  // set done=true.  30 s is a generous safety timeout.
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (true) {
    bool all_done = true;
    for (int i = 0; i < num_subs; i++) {
      if (!ctxs[i].done.load(std::memory_order_acquire)) {
        all_done = false;
        break;
      }
    }
    if (all_done || std::chrono::steady_clock::now() > deadline) break;
    sched_yield();
  }

  // ── Aggregate stats across all subscribers ──
  uint64_t total_callbacks = 0;
  AgeStats agg;
  for (const auto& ctx : ctxs) {
    total_callbacks += ctx.callback_count;
    agg.merge(ctx.stats);
  }

  // ── Cleanup ──
  a0_publisher_close(&pub);
  for (int i = 0; i < num_subs; i++) {
    a0_subscriber_close(&subs[i]);
    a0_realloc_allocator_close(&ctxs[i].alloc);
  }
  a0_file_close(&file);
  a0_file_remove(BENCH_SHM);

  // "delivery%" = fraction of (num_msgs * num_subs) possible callbacks that
  // actually fired.  < 100% means subscribers skipped messages due to lag.
  const double delivery_pct =
      100.0 * static_cast<double>(total_callbacks) /
      static_cast<double>(static_cast<uint64_t>(num_msgs) * num_subs);
  std::printf("  %-42s  delivery: %5.1f%%  age mean: %8.2f µs  worst: %8.2f µs\n",
              name, delivery_pct, agg.mean_us(), agg.max_us());
}

// ── main ──────────────────────────────────────────────────────────────────────

int main() {
  struct Suite {
    const char* name;
    int         num_subs;
    int         msg_size;
    uint64_t    deser_ns;
    int         num_msgs;
  };

  const Suite suites[] = {
      // ── A: subscriber-count sweep (no deser, 1 kB payload) ──────────────
      // More subscribers competing for the transport lock means each takes
      // longer to process callbacks, letting the publisher race further ahead.
      {"[no-deser]  1 sub  / 1kB",   1,  1024,       0,  50000},
      {"[no-deser]  4 subs / 1kB",   4,  1024,       0,  50000},
      {"[no-deser] 16 subs / 1kB",  16,  1024,       0,  50000},
      {"[no-deser] 64 subs / 1kB",  64,  1024,       0,  20000},

      // ── B: message-size sweep (16 subs, no deser) ────────────────────────
      // Larger messages fill the 16 MB ring faster.  With ITER_NEWEST,
      // wrap is handled gracefully; age shows how far behind subscribers fall.
      {"[no-deser] 16 subs /   64B",  16,          64,  0, 100000},
      {"[no-deser] 16 subs /  10kB",  16,  10 * 1024,  0,  10000},
      {"[no-deser] 16 subs /   1MB",  16, 1024*1024,   0,    500},

      // ── C: deserialization-cost sweep (16 subs, 1 kB payload) ───────────
      // A slow deserializer holds the subscriber busy while the publisher
      // races ahead.  With the transport lock released before the user
      // callback, all 16 subscribers deser concurrently — age should scale
      // with the per-callback cost, NOT with N × cost.  Without the lock-
      // release optimisation the callbacks serialise and age grows as N × cost.
      {"[deser  10µs] 16 subs / 1kB", 16, 1024,   10'000,  5000},
      {"[deser 100µs] 16 subs / 1kB", 16, 1024,  100'000,  1000},
      {"[deser   1ms] 16 subs / 1kB", 16, 1024, 1000'000,   200},
  };

  std::printf("\n%-46s  %-16s  %-24s  %s\n",
              "suite", "delivery", "mean age", "worst age");
  std::printf("%.110s\n",
              "──────────────────────────────────────────────────────────────────────────────────────────────────────────────────");
  for (const auto& suite : suites) {
    run_bench(suite.name, suite.num_subs, suite.msg_size, suite.deser_ns, suite.num_msgs);
  }
  std::printf("\n");
}
