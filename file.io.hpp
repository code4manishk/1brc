#ifndef FILE_IO_HPP__
#define FILE_IO_HPP__

#include <iostream>
#include <utility>
#include <unistd.h>
#include <filesystem>
#include <string_view>
#include <ios>
#include <utility>
#include <charconv>

#ifdef __linux__
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#endif 

#include <fstream>
#include <generator>

namespace rng = std::ranges;
namespace vw = std::views;
namespace fs = std::filesystem;

namespace io {

constexpr auto parse_digit2(const char* s, const char* e) -> float {
  float ret;
  auto [ptr, ec] = std::from_chars(s, e, ret);
  if (ec == std::errc()) return ret;
  throw std::runtime_error("invalid format");
}

constexpr auto parse_digit(const char* s, const char* e) noexcept -> float {
  int ans = 0, d = 1;
  for(auto ee = e-3; ee > s; --ee, d *= 10) {
     ans += (*ee - '0')*d;
  }
  if (*s == '-') { return -(ans + (*(e-1)-'0')/10.0f); }
  else {
    ans += (*s - '0')*d;
    return ans + (*(e-1) - '0')/10.f;
  }
}

auto generate_line(std::string_view data, char delim = '\n') -> std::generator<std::string_view> {
  for(; !std::empty(data); ) {
    const auto pos = data.find(delim);
    const bool is_last = pos == std::string_view::npos;
    co_yield is_last ? data : data.substr(0, pos);
    data.remove_prefix(is_last ? std::size(data) : pos+1);
  }
};


#ifdef __linux__
class MemoryMapped {
  public:
  constexpr MemoryMapped() = default;

  constexpr MemoryMapped(int fd, size_t len=::sysconf(_SC_PAGE_SIZE), long off = 0)
  : fd_{fd}
  , raw_data_{nullptr}
  , offset_{off}
  , num_bytes_{len}
  {
    raw_data_ = mmap(nullptr, num_bytes_, PROT_READ, MAP_PRIVATE, fd_, offset_);
    [[unlikely]] if (raw_data_ ==  MAP_FAILED) {
      std::cerr << "mmap failed" << std::endl;
      throw std::runtime_error("mmap failed");
    }
    madvise(raw_data_, num_bytes_, MADV_SEQUENTIAL);
  }

  constexpr ~MemoryMapped() noexcept {
    [[likely]] if (raw_data_ != nullptr) {
      madvise(raw_data_, num_bytes_, MADV_REMOVE|MADV_FREE);
      [[unlikely]] if (0 != munmap(raw_data_, num_bytes_))
        std::cerr << "munmap failed" << raw_data_ << " " << num_bytes_ << std::endl;
    }
  }

  constexpr auto string_view() const -> std::string_view {
    return std::string_view(static_cast<char*>(raw_data_), num_bytes_);
  }

  friend auto swap(MemoryMapped& lhs, MemoryMapped& rhs) noexcept -> void {
    std::swap(lhs.fd_, rhs.fd_);
    std::swap(lhs.raw_data_, rhs.raw_data_);
    std::swap(lhs.offset_, rhs.offset_);
    std::swap(lhs.num_bytes_, rhs.num_bytes_);
  }

  MemoryMapped(const MemoryMapped&) = delete;
  MemoryMapped& operator = (const MemoryMapped&) = delete;

  constexpr MemoryMapped(MemoryMapped&& rhs) noexcept
  {
    fd_ = std::exchange(rhs.fd_, -1);
    num_bytes_ = std::exchange(rhs.num_bytes_, 0);
    offset_ = std::exchange(rhs.offset_, 0);
    raw_data_ = std::exchange(rhs.raw_data_, nullptr);
  }

  constexpr MemoryMapped& operator = (MemoryMapped&& rhs) noexcept {
    fd_ = std::exchange(rhs.fd_, -1);
    num_bytes_ = std::exchange(rhs.num_bytes_, 0);
    offset_ = std::exchange(rhs.offset_, 0);
    raw_data_ = std::exchange(rhs.raw_data_, nullptr);
    return *this;
  }

  private:
  int fd_{-1};
  void* raw_data_{nullptr};
  long offset_{0};
  size_t num_bytes_{0};
};
#endif

class FileReader {
  public:
  constexpr explicit FileReader(const std::filesystem::path& file_path)
  : file_path_{file_path}
  , fd_{-1}
  , num_bytes_{0}
  , error_{}
  {
    if (!std::filesystem::exists(file_path_))
      throw std::runtime_error("file_path not found");
    fd_ = ::open(file_path_.string().c_str(), O_RDONLY|O_DIRECT|O_SYNC);
    if (fd_ == -1) throw std::runtime_error("couldn't open file for mmap");
    struct stat file_stat;
    if (-1 == fstat(fd_, &file_stat)) throw std::runtime_error("could fstat file for mmap");  
    num_bytes_ = file_stat.st_size;                                     
  }

  FileReader(const FileReader&) = delete;
  FileReader& operator = (const FileReader&) = delete;
  FileReader(FileReader&&) = default;
  FileReader& operator = (FileReader&&) = default;

  constexpr auto mmap() -> MemoryMapped {
    return MemoryMapped(fd_, num_bytes_, 0);
  }

  auto generate_mmap(size_t len) -> std::generator<MemoryMapped> {
    for(size_t off = 0; off < num_bytes_; off += len)
      co_yield MemoryMapped(fd_, std::min(len, num_bytes_-off), off);
  }

  auto generate_str(size_t len) -> std::generator<std::string> {
    std::ifstream ifs(file_path_, std::ios::in);
    if (ifs) {
      for(size_t off = 0; off < num_bytes_; off += len) {
        std::string str(len, '\0');
        ifs.seekg(off, std::ios::beg);
        if (ifs.read(str.data(), len)) co_yield std::move(str);
      }
    }
  }

  auto read_line(char delim = '\n') -> std::generator<std::string_view> {
    for(auto&& line : io::generate_line(str_, delim)) co_yield line;
  }

  constexpr ~FileReader() noexcept {
    ::close(fd_);
  }

  protected:
  constexpr auto read() -> std::string_view {
    if(str_.empty()) {
      std::ifstream file(file_path_, std::ios::in | std::ios::binary | std::ios::ate);
      if (file) {
        auto fileSize = file.tellg();

        file.seekg(0, std::ios::beg);
        str_.resize(fileSize);

        if (!file.read(str_.data(), fileSize)) {
            str_.clear();
        }
      }
      else 
        error_ = "Error: file in bad state";
    }
    if (!error_.empty()) throw std::runtime_error("read failed");
    return str_;
  }

  private:
  std::filesystem::path file_path_;
  int fd_;
  size_t num_bytes_;
  std::string str_;
  std::string error_;
};

} // namespace io


#endif // FILE_IO_HPP__
