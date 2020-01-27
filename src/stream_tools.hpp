#pragma once

#include <a0/alloc.h>
#include <a0/stream.h>

#include <functional>
#include <thread>

#include "sync.hpp"

static const char kClock[] = "a0_clock";

namespace a0 {

inline a0_buf_t buf(a0_stream_frame_t frame) {
  return a0_buf_t{
      .ptr = frame.data,
      .size = frame.hdr.data_size,
  };
}

// TODO: maybe specialize std::unique_lock.
struct sync_stream_t {
  a0_stream_t* stream{nullptr};

  template <typename Fn>
  auto with_lock(Fn&& fn) {
    struct guard {
      a0_locked_stream_t lk;
      guard(a0_stream_t* stream) {
        a0_lock_stream(stream, &lk);
      }
      ~guard() {
        a0_unlock_stream(lk);
      }
    } scope(stream);
    return fn(scope.lk);
  }
};

inline a0_alloc_t stream_allocator(a0_locked_stream_t* lk) {
  return a0_alloc_t{
      .user_data = lk,
      .fn =
          [](void* data, size_t size, a0_buf_t* out) {
            a0_stream_frame_t frame;
            a0_stream_alloc(*(a0_locked_stream_t*)data, size, &frame);
            *out = buf(frame);
          },
  };
}

struct stream_thread {
  struct state_t {
    a0_stream_t stream;
    std::thread::id t_id;

    std::function<void(a0_locked_stream_t)> on_stream_nonempty;
    std::function<void(a0_locked_stream_t)> on_stream_hasnext;

    a0::sync<std::function<void()>> onclose;

    bool handle_first_pkt() {
      sync_stream_t ss{&stream};
      return ss.with_lock([&](a0_locked_stream_t slk) {
        if (a0_stream_await(slk, a0_stream_nonempty)) {
          return false;
        }

        on_stream_nonempty(slk);

        return true;
      });
    }

    bool handle_next_pkt() {
      sync_stream_t ss{&stream};
      return ss.with_lock([&](a0_locked_stream_t slk) {
        if (a0_stream_await(slk, a0_stream_has_next)) {
          return false;
        }

        on_stream_hasnext(slk);

        return true;
      });
    }

    void thread_main() {
      if (handle_first_pkt()) {
        while (handle_next_pkt()) {
        }
      }

      onclose.with_lock([](auto* fn) {
        if (*fn) {
          (*fn)();
        }
      });
    }
  };

  std::shared_ptr<state_t> state;

  errno_t init(a0_buf_t arena,
               a0_stream_protocol_t stream_protocol,
               std::function<errno_t(a0_locked_stream_t, a0_stream_init_status_t)> on_stream_init,
               std::function<void(a0_locked_stream_t)> on_stream_nonempty,
               std::function<void(a0_locked_stream_t)> on_stream_hasnext) {
    state = std::make_shared<state_t>();
    state->on_stream_nonempty = on_stream_nonempty;
    state->on_stream_hasnext = on_stream_hasnext;

    a0_stream_init_status_t init_status;
    a0_locked_stream_t slk;
    a0_stream_init(&state->stream, arena, stream_protocol, &init_status, &slk);
    errno_t err = on_stream_init(slk, init_status);
    a0_unlock_stream(slk);
    if (err) {
      return err;
    }
    std::thread t([state_ = state]() {
      state_->thread_main();
    });
    state->t_id = t.get_id();
    t.detach();
    return A0_OK;
  }

  errno_t async_close(std::function<void()> onclose) {
    if (!state) {
      return ESHUTDOWN;
    }

    state->onclose.set(onclose);
    a0_stream_close(&state->stream);

    return A0_OK;
  }

  errno_t await_close() {
    if (!state) {
      return ESHUTDOWN;
    }
    if (std::this_thread::get_id() == state->t_id) {
      return EDEADLK;
    }

    a0::Event close_event;
    async_close([&]() {
      close_event.set();
    });
    close_event.wait();

    return A0_OK;
  }
};

}  // namespace a0
