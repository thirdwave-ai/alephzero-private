// Fan-out pub/sub benchmark
//
// For every (num_subscribers, msg_size, deser_ns) combination the benchmark:
//   - Initialises N async subscribers, each awaiting new messages.
//   - Publishes one message per picobench iteration.
//   - Spin-waits until every subscriber callback has fired for that message.
//
// deser_ns > 0 simulates expensive per-message deserialization inside the
// subscriber callback.  Because a0_subscriber releases the transport lock
// before invoking the user callback, each subscriber thread runs its
// simulated work concurrently — this exercises the robustness of the
// lock-release path introduced for slow deserialization scenarios.
//
// After each suite picobench prints throughput; additionally, per-iteration
// "time-to-full-delivery" (from a0_pub call until the last subscriber fires)
// is tracked and reported as mean and worst-case latency.

#define PICOBENCH_STD_FUNCTION_BENCHMARKS
#define PICOBENCH_IMPLEMENT
// Prevent picobench from pinning the runner thread to CPU 0 via
// sched_setaffinity.  Without this, all subscriber threads created inside
// the benchmark lambda inherit the single-CPU affinity mask and are forced
// to share one core, serialising all callback work and inflating fan-out
// latency by N× the per-callback cost.
#define PICOBENCH_DONT_BIND_TO_ONE_CORE

#include <a0.h>
#include <picobench/picobench.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <sched.h>
#include <string>
#include <vector>

static const char BENCH_SHM[] = "bench_pubsub.shm";

template <typename T>
static inline __attribute__((always_inline)) void use(const T& t) {
  asm volatile(""
               :
               : "r,m"(t)
               : "memory");
}

// Per-subscriber context: an independent realloc allocator (so each sub
// owns its own packet buffer), a pointer to the shared receive counter, and
// the optional simulated deserialization cost in nanoseconds.
struct SubCtx {
  a0_alloc_t alloc;
  std::atomic<int>* total_received;
  uint64_t deser_ns{0};  // 0 = no simulated deserialization
};

// Simulate deserialization by iterating over the packet payload bytes.
// Using a volatile accumulator and asm fence prevents the compiler from
// eliding the work.  The loop runs until at least `deser_ns` nanoseconds
// have elapsed, reading payload bytes in a round-robin fashion so the cost
// scales naturally with a real parser.
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

// Accumulates per-iteration time-to-full-delivery statistics.
// "Full delivery" = elapsed time from a0_pub until every subscriber callback
// has fired (i.e., the fan-out wait-time seen by the publisher thread).
struct WaitStats {
  uint64_t sum_ns = 0;
  uint64_t max_ns = 0;
  int count = 0;

  double mean_us() const { return count ? (static_cast<double>(sum_ns) / count) / 1e3 : 0.0; }
  double max_us()  const { return static_cast<double>(max_ns) / 1e3; }
};

// Returns a picobench benchmark that publishes one message per iteration and
// waits for all `num_subs` subscriber callbacks to fire before moving on.
// `deser_ns` injects a simulated deserialization cost into each callback.
// `stats` is updated with per-iteration wait durations (caller must ensure
// its lifetime covers the picobench runner's run()).
auto bench_pubsub_fanout(int num_subs, int msg_size, uint64_t deser_ns, WaitStats& stats) {
  return [num_subs, msg_size, deser_ns, &stats](picobench::state& s) {
    a0_file_remove(BENCH_SHM);
    a0_file_t file;
    a0_file_open(BENCH_SHM, nullptr, &file);

    // Monotonically-increasing count of all callbacks fired across every
    // subscriber and every iteration.  Iteration i is complete when:
    //   total_received >= (i + 1) * num_subs
    std::atomic<int> total_received{0};

    // Initialise subscribers.
    std::vector<SubCtx> ctxs(num_subs);
    std::vector<a0_subscriber_t> subs(num_subs);

    for (int i = 0; i < num_subs; i++) {
      a0_realloc_allocator_init(&ctxs[i].alloc);
      ctxs[i].total_received = &total_received;
      ctxs[i].deser_ns = deser_ns;

      a0_packet_callback_t cb = {
          .user_data = &ctxs[i],
          .fn =
              [](void* user_data, a0_packet_t pkt) {
                auto* ctx = static_cast<SubCtx*>(user_data);
                // Simulate deserialization before signalling completion so
                // that the fan-out wait time includes the callback work.
                simulate_deser(pkt, ctx->deser_ns);
                ctx->total_received->fetch_add(1, std::memory_order_release);
              },
      };

      a0_subscriber_init(
          &subs[i], file.arena, ctxs[i].alloc, A0_INIT_AWAIT_NEW, A0_ITER_NEXT, cb);
    }

    // Publisher: reuse the same packet every iteration.
    a0_publisher_t pub;
    a0_publisher_init(&pub, file.arena);

    std::string payload(msg_size, 'x');
    a0_packet_t pkt;
    a0_packet_init(&pkt);
    pkt.payload = {.ptr = (uint8_t*)payload.data(), .size = (size_t)msg_size};

    // Warmup: publish one message and wait for every subscriber to respond
    // before the timed loop begins.  This ensures all internal subscriber
    // threads have been scheduled and are blocking on the transport by the
    // time we start measuring — otherwise the first iteration absorbs thread
    // startup latency and inflates the worst-case figure.
    a0_pub(&pub, pkt);
    while (total_received.load(std::memory_order_acquire) < num_subs) {
      sched_yield();
    }
    total_received.store(0, std::memory_order_release);

    int iter_idx = 0;
    for (auto&& _ : s) {
      use(_);
      ++iter_idx;
      const int target = iter_idx * num_subs;

      // Time from publish until the last subscriber callback fires.
      const auto t0 = std::chrono::steady_clock::now();
      a0_pub(&pub, pkt);

      // Spin until every subscriber has received this iteration's message.
      while (total_received.load(std::memory_order_acquire) < target) {
        sched_yield();
      }
      const auto t1 = std::chrono::steady_clock::now();

      const uint64_t ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
      stats.sum_ns += ns;
      if (ns > stats.max_ns) stats.max_ns = ns;
      stats.count++;
    }

    // Cleanup: drain subscriber threads before releasing their allocators.
    a0_publisher_close(&pub);
    for (int i = 0; i < num_subs; i++) {
      a0_subscriber_close(&subs[i]);
      a0_realloc_allocator_close(&ctxs[i].alloc);
    }
    a0_file_close(&file);
    a0_file_remove(BENCH_SHM);
  };
}

int main() {
  struct Suite {
    const char* name;
    int         num_subs;
    int         msg_size;
    uint64_t    deser_ns;  // simulated per-callback deserialization cost
    int         iters;
  };

  // Three sweeps (all without deserialization cost):
  //   A) Vary subscriber count  (fixed 1 kB payload, no deser)
  //   B) Vary message size      (fixed 16 subscribers, no deser)
  // One sweep with simulated deserialization:
  //   C) Vary deser cost        (fixed 16 subscribers, 1 kB payload)
  //      Tests robustness of the lock-release-before-callback path.
  const Suite suites[] = {
      // --- A: subscriber-count sweep (no deser) ---
      {"[no-deser] 1 sub  / 1kB",   1,  1024,       0,  50000},
      {"[no-deser] 4 subs / 1kB",   4,  1024,       0,  20000},
      {"[no-deser] 16 subs / 1kB", 16,  1024,       0,  10000},
      {"[no-deser] 64 subs / 1kB", 64,  1024,       0,   5000},
      // --- B: message-size sweep (no deser) ---
      {"[no-deser] 16 subs /  64B",  16,           64,  0,  20000},
      {"[no-deser] 16 subs /  1kB",  16,         1024,  0,  10000},
      {"[no-deser] 16 subs / 10kB",  16,   10 * 1024,  0,   5000},
      {"[no-deser] 16 subs /  1MB",  16, 1024 * 1024,  0,    500},
      // --- C: deserialization-cost sweep (16 subs, 1 kB payload) ---
      // Each callback busy-spins on the payload for the stated duration,
      // simulating a slow deserializer.  Because the transport lock is
      // released before the user callback, all subscriber threads run
      // concurrently; fan-out time should stay close to the per-callback
      // cost rather than N * cost.
      {"[deser  10µs] 16 subs / 1kB", 16, 1024,   10'000,  5000},
      {"[deser 100µs] 16 subs / 1kB", 16, 1024,  100'000,  1000},
      {"[deser   1ms] 16 subs / 1kB", 16, 1024, 1000'000,   200},
  };

  for (const auto& suite : suites) {
    WaitStats stats;
    picobench::runner r;
    r.set_suite(suite.name);
    r.add_benchmark(
        suite.name,
        bench_pubsub_fanout(suite.num_subs, suite.msg_size, suite.deser_ns, stats))
        .iterations({suite.iters});
    r.run();
    std::printf("  [wait] mean: %8.2f µs  |  worst: %8.2f µs  (%d iters)\n",
                stats.mean_us(), stats.max_us(), stats.count);
  }
}
