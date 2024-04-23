#include "src/pmparser.h"

#include <doctest.h>
#include <cerrno>

#include "src/test_util.hpp"

TEST_CASE("pmparser] errno robustness") {
  // Set errno going in to the function.
  // Ensure that we still get a valid pmparse.
  errno = ENOENT;
  auto parser = pmparser_parse(-1);
  REQUIRE_NE(parser, static_cast<procmaps_iterator*>(NULL));
  pmparser_free(parser);
}
