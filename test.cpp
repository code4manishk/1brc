#include <print>
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <iterator>
#include <string>
#include <string_view>
#include <ranges>
#include <functional>
#include <generator>
#include <filesystem>
#include <deque>
#include <thread>
#include <execution>
#include <map>
#include <set>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <barrier>
#include <stop_token>
#include <syncstream>
#include <optional>

#include "absl/container/flat_hash_map.h"

#include "file.io.hpp"

using namespace std;

template<typename T = float>
struct DataPoint {
  struct MetaInfo {
    constexpr auto update(std::convertible_to<T> auto && v) noexcept -> void {
      if (mn > v) mn = v;
      if (mx < v) mx = v;

      tot += v;
      ++n;
    }

    constexpr auto operator + (const MetaInfo& rhs) noexcept -> MetaInfo {
      MetaInfo tmp{*this};

      tmp.mn = std::min(tmp.mn, rhs.mn);
      tmp.mx = std::max(tmp.mx, rhs.mx);
      tmp.tot += rhs.tot;
      tmp.n += rhs.n;
      return tmp;
    }

    T mn{std::numeric_limits<T>::max()};
    T mx{std::numeric_limits<T>::min()};
    T tot{0.0};
    size_t n{0};
  };

  constexpr DataPoint() = default;

  DataPoint(const DataPoint&) = delete;
  auto operator = (const DataPoint&) -> DataPoint& = delete;

  constexpr DataPoint(DataPoint&&) = default;
  constexpr auto operator = (DataPoint&&) -> DataPoint& = default;
  constexpr ~DataPoint() noexcept = default;

  constexpr void accept(std::convertible_to<T> auto&& p) noexcept {
    auto& v = points.emplace_back(std::forward<decltype(p)>(p));
    meta.update(v);
  }

  constexpr auto meta_view() const noexcept -> MetaInfo {
    return meta;
  }

  std::deque<T> points{};
  MetaInfo meta{};
};

template<typename InType=char, typename OutType=float, size_t N = 32768>
class Database {
  public:
  using output_storage_t = absl::flat_hash_map<std::basic_string<InType>, DataPoint<OutType>>;

  constexpr explicit Database(const fs::path& input_path)
  : input_path_{input_path}
  , input_(input_path_)
  , output_{}
  {
  }

  constexpr auto find(const std::basic_string_view<InType>& name) const -> DataPoint<OutType>::MetaInfo {
    return std::transform_reduce(std::execution::par,
            cbegin(output_), cend(output_),
            typename DataPoint<OutType>::MetaInfo{},
            plus{},
            [&name](auto&& mp) noexcept -> DataPoint<OutType>::MetaInfo { 
               if (auto it = mp.find(name); it != cend(mp)) return it->second.meta_view();
               else return {};
            });
  }

  constexpr auto process_input2(unsigned num_workers = std::thread::hardware_concurrency()) noexcept -> void {
    const auto chunk_size = 4*1024*::sysconf(_SC_PAGE_SIZE);
    auto on_completion = [] { };
    std::timed_mutex mu;
    std::condition_variable_any cond_add, cond_get;
    std::barrier barrier{num_workers+2, on_completion};
    deque<io::MemoryMapped> data;
    stop_source stop;
    vector<thread> workers;
    std::string overflow;
    int waiting = 0;
    const auto idx = vw::iota(0) | vw::take(num_workers) | rng::to<vector>();
    generate_n(back_inserter(output_), num_workers, [] { return output_storage_t{N}; });

    overflow.reserve(chunk_size);

    auto producer = [&] (stop_token st) noexcept -> void {
      const auto min_M = 2*num_workers , max_M = 5*num_workers;
      auto M = 3*num_workers;

      while(!st.stop_requested()) {
        for(auto&& str : input_.generate_mmap(chunk_size)) {
          const auto chunk = str.string_view();
          auto s = chunk.find('\n');
          auto e = chunk.rfind('\n', chunk.size());
          overflow.append(chunk, 0, s+1);
          overflow.append(chunk, e+1);
          bool underflow = false;
          do {
            unique_lock lk{mu};
            //cerr << waiting << ',' << std::size(data) << '/' << M << endl;
            if (underflow = cond_add.wait(lk, st, [&] () noexcept { return waiting or std::size(data) < M; }); underflow) {
              data.emplace_back(std::move(str));
              break;
            }
          } while(underflow == false);

          M = waiting ? min(max_M, M+1) : max(min_M, M-1);
          cond_get.notify_all();
        }
        {
          unique_lock lk{mu};
          data.emplace_back(io::MemoryMapped{});
        }
        cond_get.notify_all();
        barrier.arrive_and_wait();
      }
    };

    auto process_chunk = [&](string_view chunk, int i) noexcept -> void {
      auto& out = output_[i];
      while(!chunk.empty()) {
        auto s = chunk.find(';');
        auto e = chunk.find('\n', s+1);
        const bool is_last = e == string_view::npos;
        out[chunk.substr(0, s)].accept(io::parse_digit(chunk.data()+s+1, chunk.data()+(is_last ? chunk.size() : e)));
        chunk.remove_prefix(is_last ? chunk.size() : e+1);
      }
    };

    auto consumer = [&](stop_token st, int i) noexcept -> void {
      while(!st.stop_requested()) {
        std::optional<io::MemoryMapped> oval{nullopt};
        {
          unique_lock lk{mu};
          ++waiting;
          if (cond_get.wait(lk, st, [&] () noexcept { return !rng::empty(data); })) {
            oval = std::move(data.front());
            data.pop_front();
          }
          --waiting;
        }
        cond_add.notify_one();
        const auto str = oval ? oval->string_view() : string_view{};
        if (str.empty()) {
          process_chunk(overflow, i);
          overflow.clear();
          stop.request_stop();
          cond_get.notify_all();
          break;
        }
        auto s = str.find('\n');
        auto e = str.rfind('\n', str.size());
        process_chunk(string_view{str.begin()+s+1, str.begin()+e}, i);
      }
      barrier.arrive_and_wait();
    };

    rng::transform(idx, back_inserter(workers), [&] (auto i) { return thread{consumer, stop.get_token(), i}; });
    workers.emplace_back(thread{producer, stop.get_token()});

    barrier.arrive_and_wait();
    rng::for_each(workers, [](auto& th) { th.join(); });
  }

  constexpr auto process_input(unsigned num_workers = std::thread::hardware_concurrency()) noexcept -> void {
    auto mapped_data = input_.mmap();
    const auto chunks = generate_chunks(mapped_data.string_view(), num_workers) | rng::to<std::vector>();
    const auto idx = vw::iota(0) | vw::take(size(chunks)) | rng::to<vector>();

    generate_n(back_inserter(output_), size(chunks), [] { return output_storage_t{N}; });

    std::for_each(std::execution::par_unseq, rng::begin(idx), rng::end(idx), [&](auto i) noexcept {
      auto values = chunks[i] |
                  vw::split('\n') |
                  vw::filter([](auto&& r) noexcept { return !rng::empty(r); }) |
                  vw::transform([](auto&& toks) noexcept { return string_view(toks); }) |
                  vw::transform([](auto&& w) noexcept {
                    auto m = w.find(';');
                    return make_pair(w.substr(0, m), io::parse_digit(w.data()+m+1, w.data()+w.size()));
                  });
      rng::for_each(values | vw::as_const, [&out=output_[i]](auto&& p) noexcept { out[p.first].accept(p.second); });
    });
  }

  auto keys() const -> decltype(auto) {
    auto all_keys = output_ | vw::transform([](auto&& o) { return o | vw::keys; }) | vw::join;
    auto entries = rng::fold_left(all_keys, unordered_set<std::string_view>{},
                    [](auto&& st, auto&& v) { st.emplace(v); return std::move(st); }) |
                    rng::to<vector>();

    rng::sort(entries);
    return entries;
  }

  protected:
  auto generate_chunks(string_view data, unsigned counts, char sep = '\n') -> std::generator<std::string_view> {
    auto sz = std::max<size_t>(1, std::size(data) / counts);
    for(; !data.empty();) {
      auto pos = data.find(sep, sz);
      const bool is_last = pos == std::string_view::npos;
      co_yield is_last ? data : data.substr(0, pos);
      data.remove_prefix(is_last ? std::size(data) : pos+1);
    }
  }

  private:
  fs::path input_path_;
  io::FileReader input_;
  std::vector<output_storage_t> output_;
};

int main(int argc, const char *const argv[]) {
  static_assert(__cplusplus >= 202302, "c++23 is needed");
  std::ios::sync_with_stdio(false);

  if (argc < 2) {
    println("file path needed");
    return 1;
  }

  Database result{argv[1]};
  result.process_input2();

  auto formatted_result = result.keys() |
        vw::transform([&result](auto&& key) noexcept {
          auto out = result.find(key);
          return std::format("{}={:.1f}/{:.1f}/{:.1f}", key, out.mn, out.tot/out.n, out.mx);
        }) |
        vw::join_with(string(", "));

  rng::copy(formatted_result, std::ostreambuf_iterator<char>(cout));

  return 0;
}
