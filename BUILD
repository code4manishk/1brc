cc_binary(
  name="challenge",
  deps = ["@abseil-cpp//absl/container:flat_hash_map"],
  srcs=["test.cpp", "file.io.hpp"],
  cxxopts=["--std=c++23", "-Ofast", "-DPARALLEL", "-D_GLIBCXX_ASSERTIONS"],
  linkopts=["-ltbb", "-lc"]
)
