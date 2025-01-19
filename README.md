It will require c++23 compatible compiler, abseil cpp, and tbb lib. Currently, flat_map implementation is not available in the stl lib, so I've used abseil flat_hash_map
Try it on your machine and fork it and improve it. post your runtime for 1 billion datapoints ;)
create the dataset as following, I have added a small 1 million datapoints as an example.
python create_measurements.py cities.txt data.1000.txt 1000000000
bazel build //...
time ./bazel-bin/challenge data.1000.txt
