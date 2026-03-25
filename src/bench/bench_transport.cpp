#define PICOBENCH_STD_FUNCTION_BENCHMARKS
#define PICOBENCH_IMPLEMENT

#include <a0.h>
#include <picobench/picobench.hpp>

static const char BENCH_SHM[] = "bench.shm";

template <typename T>
static inline __attribute__((always_inline)) void use(const T& t) {
  asm volatile(""
               :
               : "r,m"(t)
               : "memory");
}

struct BenchFixture {
  BenchFixture() {
    a0_file_remove(BENCH_SHM);
    a0_file_open(BENCH_SHM, nullptr, &file);

    a0_transport_init_status_t init_status;
    // a0_transport_init leaves the transport locked. Release immediately so
    // that benchmarks that exercise the full lock/alloc/commit/unlock cycle
    // per iteration don't accumulate lock-hold time across all iterations and
    // trigger spurious "A0 Transport lock held for" warnings.
    a0_locked_transport_t lk;
    a0_transport_init(&transport, file.arena, &init_status, &lk);
    a0_transport_unlock(&lk);
  }

  ~BenchFixture() {
    a0_transport_close(&transport);
    a0_file_close(&file);
    a0_file_remove(BENCH_SHM);
  }

  a0_file_t file;
  a0_transport_t transport;
};

auto bench_memcpy(int msg_size) {
  return [msg_size](picobench::state& s) {
    BenchFixture fixture;
    (void)fixture;

    std::string src(msg_size, 0);
    char* dst = (char*)malloc(msg_size);
    for (auto&& _ : s) {
      use(_);
      memcpy(dst, src.data(), msg_size);
    }
    free(dst);
  };
}

auto bench_memcpy_slots(int msg_size) {
  return [msg_size](picobench::state& s) {
    BenchFixture fixture;
    (void)fixture;

    int slots = A0_FILE_OPTIONS_DEFAULT.create_options.size / msg_size;
    char** array = (char**)malloc(slots * sizeof(char*));
    for (int i = 0; i < slots; i++) {
      array[i] = (char*)malloc(msg_size);
    }
    std::string src(msg_size, 0);
    int slot = 0;
    for (auto&& _ : s) {
      use(_);
      memcpy(array[slot], src.data(), msg_size);
      slot = (slot + 1) % slots;
    }
    for (int i = 0; i < slots; i++) {
      free(array[i]);
    }
    free(array);
  };
}

auto bench_malloc(int msg_size) {
  return [msg_size](picobench::state& s) {
    BenchFixture fixture;
    (void)fixture;

    for (auto&& _ : s) {
      use(_);
      void* ptr = malloc(msg_size);
      use(ptr);
      free(ptr);
    }
  };
}

auto bench_malloc_slots(int msg_size) {
  return [msg_size](picobench::state& s) {
    BenchFixture fixture;
    (void)fixture;

    int slots = A0_FILE_OPTIONS_DEFAULT.create_options.size / msg_size;
    char** array = (char**)malloc(slots * sizeof(char*));
    for (int i = 0; i < slots; i++) {
      array[i] = (char*)malloc(msg_size);
    }

    int slot = 0;
    for (auto&& _ : s) {
      use(_);
      free(array[slot]);
      array[slot] = (char*)malloc(msg_size);
      slot = (slot + 1) % slots;
    }

    for (int i = 0; i < slots; i++) {
      free(array[i]);
    }
    free(array);
  };
}

auto bench_malloc_memcpy_slots(int msg_size) {
  return [msg_size](picobench::state& s) {
    BenchFixture fixture;
    (void)fixture;

    int slots = A0_FILE_OPTIONS_DEFAULT.create_options.size / msg_size;
    char** array = (char**)malloc(slots * sizeof(char*));
    for (int i = 0; i < slots; i++) {
      array[i] = (char*)malloc(msg_size);
    }
    std::string src(msg_size, 0);

    int slot = 0;
    for (auto&& _ : s) {
      use(_);
      free(array[slot]);
      array[slot] = (char*)malloc(msg_size);
      memcpy(array[slot], src.data(), msg_size);
      slot = (slot + 1) % slots;
    }

    for (int i = 0; i < slots; i++) {
      free(array[i]);
    }
    free(array);
  };
}

auto bench_a0_alloc(int msg_size) {
  return [msg_size](picobench::state& s) {
    BenchFixture fixture;

    // Each iteration acquires the transport lock, allocates one frame, commits
    // the ring-buffer metadata, and releases the lock — matching the real
    // publisher path (a0_pub_raw).  Holding the lock across all iterations
    // would accumulate lock-hold time equal to the entire benchmark duration
    // and trigger spurious "A0 Transport lock held for" warnings.
    for (auto&& _ : s) {
      use(_);
      a0_locked_transport_t lk;
      a0_transport_lock(&fixture.transport, &lk);
      a0_transport_frame_t frame;
      a0_transport_alloc(lk, msg_size, &frame);
      a0_transport_commit(lk);
      a0_transport_unlock(&lk);
    }
  };
}

auto bench_a0_alloc_memcpy(int msg_size) {
  return [msg_size](picobench::state& s) {
    BenchFixture fixture;

    // Same per-iteration lock discipline as bench_a0_alloc.  The memcpy
    // of the payload into the transport frame happens under the lock — this
    // is the publisher's critical section.  For the subscriber side the
    // analogous memcpy was moved outside the lock via the seqlock mechanism
    // (commits 8cfa850 and 5c370ee).  The publisher path does not yet
    // support an out-of-lock write because releasing the lock before commit
    // causes a0_transport_lock to reset the working page, discarding the
    // uncommitted allocation.  The per-message lock hold time shown here is
    // the true cost the publisher imposes on concurrent readers.
    std::string src(msg_size, 0);
    for (auto&& _ : s) {
      use(_);
      a0_locked_transport_t lk;
      a0_transport_lock(&fixture.transport, &lk);
      a0_transport_frame_t frame;
      a0_transport_alloc(lk, msg_size, &frame);
      memcpy(frame.data, src.data(), msg_size);
      a0_transport_commit(lk);
      a0_transport_unlock(&lk);
    }
  };
}

int main() {
  struct suite {
    std::string name;
    int msg_size;
    int iter;
  };
  std::vector<suite> suites;
  suites.push_back({"64B msgs", 64, (int)2e7});
  suites.push_back({"1kB msgs", 1024, (int)1e7});
  suites.push_back({"10kB msgs", 10 * 1024, (int)2e6});
  suites.push_back({"1MB msgs", 1024 * 1024, (int)1e4});
  suites.push_back({"4MB msgs", 4 * 1024 * 1024, (int)2e3});

  for (auto&& suite : suites) {
    picobench::runner r;

    auto malloc_group = suite.name + " : malloc compare";
    r.set_suite(malloc_group.c_str());
    r.add_benchmark("malloc", bench_malloc(suite.msg_size)).iterations({suite.iter});
    r.add_benchmark("malloc_slots", bench_malloc_slots(suite.msg_size)).iterations({suite.iter});
    r.add_benchmark("a0_alloc", bench_a0_alloc(suite.msg_size)).iterations({suite.iter});

    auto memcpy_group = suite.name + " : memcpy compare";
    r.set_suite(memcpy_group.c_str());
    r.add_benchmark("memcpy", bench_memcpy(suite.msg_size)).iterations({suite.iter});
    r.add_benchmark("memcpy_slots", bench_memcpy_slots(suite.msg_size)).iterations({suite.iter});
    r.add_benchmark("malloc_memcpy_slots", bench_malloc_memcpy_slots(suite.msg_size))
        .iterations({suite.iter});
    r.add_benchmark("a0_alloc_memcpy", bench_a0_alloc_memcpy(suite.msg_size))
        .iterations({suite.iter});

    r.run();
  }
}
