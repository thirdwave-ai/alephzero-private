#pragma once

#include <a0/common.h>
#include <a0/packet.h>

#include <map>
#include <mutex>
#include <set>
#include <string>
#include <unistd.h>

#include "src/sync.hpp"
#include "src/transport_tools.hpp"

namespace a0 {
namespace test {

inline std::string str(a0_buf_t buf) {
  return std::string((char*)buf.ptr, buf.size);
}

inline std::string str(a0_transport_frame_t frame) {
  return str(a0::buf(frame));
}

inline a0_buf_t buf(std::string str) {
  static sync<std::set<std::string>> memory;
  return memory.with_lock([&](auto* mem) {
    if (!mem->count(str)) {
      mem->insert(str);
    }
    return a0_buf_t{
        .ptr = (uint8_t*)mem->find(str)->c_str(),
        .size = str.size(),
    };
  });
}

inline a0_alloc_t allocator() {
  static sync<std::map<size_t, std::string>> data;

  return (a0_alloc_t){
      .user_data = &data,
      .fn =
          [](void* user_data, size_t size, a0_buf_t* out) {
            auto* data = (sync<std::map<size_t, std::string>>*)user_data;
            data->with_lock([&](auto* dump) {
              auto key = dump->size();
              (*dump)[key].resize(size);
              out->size = size;
              out->ptr = (uint8_t*)((*dump)[key].c_str());
            });
          },
  };
};

inline a0_packet_headers_block_t header_block(a0_packet_header_t* hdr) {
  return a0_packet_headers_block_t{
      .headers = hdr,
      .size = 1,
      .next_block = NULL,
  };
}

inline bool is_valgrind() {
#ifdef RUNNING_ON_VALGRIND
  return RUNNING_ON_VALGRIND;
#endif
  char* env = getenv("RUNNING_ON_VALGRIND");
  return env && std::string(env) != "0";
}

template <typename Fn>
inline pid_t subproc(Fn&& fn) {
  pid_t pid = fork();
  if (!pid) {
    fn();
    exit(0);
  }
  return pid;
}

}  // namespace test
}  // namespace a0

#define REQUIRE_OK(err) REQUIRE((err) == A0_OK);
#define REQUIRE_EXIT(FN_BODY)                                            \
  {                                                                      \
    int _ret_code_##__LINE__;                                            \
    waitpid(a0::test::subproc([&]() FN_BODY), &_ret_code_##__LINE__, 0); \
    REQUIRE(WIFEXITED(_ret_code_##__LINE__));                            \
  }
