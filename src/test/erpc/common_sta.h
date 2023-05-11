#include <stdio.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <vector>
#include <random>

#include "eRPC/erpc.h"

static const std::string kServerHostname = "192.168.1.52";
static const std::string kClientHostname = "192.168.1.51";

static constexpr uint16_t kServerUDPPort = 31850;
static constexpr uint16_t kClientUDPPort = 31851;
static constexpr uint8_t kReqType = 2;
static constexpr uint8_t kStopType = 3;
static constexpr uint8_t kSimpleReqType=4;
static constexpr size_t kMsgSize = 64;

#define ONESIDE_EXT 1

struct AppContext {
  std::vector<erpc::MsgBufferWrap> reqs;
  std::vector<erpc::MsgBufferWrap> resps;
  volatile size_t cnt;
};

struct APlusBReq {
  int a;
  int b;
};

struct APlusBResp {
  int result;
};

struct SimpleDataReq {
  size_t s;
};

struct SimpleDataResp {};

struct Params {
  int thread_num;
  size_t op_per_thread;
  size_t batch_size;
  size_t sess_num;
  size_t payload;
};

struct Stat {
  std::atomic<size_t> elapse{};
};

struct ServerContext {
  erpc::IBRpcWrap *rpc;
  int thread_idx;
  volatile bool running;
};

class SlowRand {
  std::random_device rand_dev_;  // Non-pseudorandom seed for twister
  std::mt19937_64 mt_;
  std::uniform_int_distribution<uint64_t> dist_;

 public:
  SlowRand() : mt_(rand_dev_()), dist_(0, UINT64_MAX) {}

  inline uint64_t next_u64() { return dist_(mt_); }
};

class FastRand {
 public:
  uint64_t seed_;

  /// Create a FastRand using a seed from SlowRand
  FastRand() {
    SlowRand slow_rand;
    seed_ = slow_rand.next_u64();
  }

  inline uint32_t next_u32() {
    seed_ = seed_ * 1103515245 + 12345;
    return static_cast<uint32_t>(seed_ >> 32);
  }
};

class ChronoTimer {
 public:
  ChronoTimer() { reset(); }
  void reset() { start_time_ = std::chrono::high_resolution_clock::now(); }

  /// Return seconds elapsed since this timer was created or last reset
  double get_sec() const { return get_ns() / 1e9; }

  /// Return milliseconds elapsed since this timer was created or last reset
  double get_ms() const { return get_ns() / 1e6; }

  /// Return microseconds elapsed since this timer was created or last reset
  double get_us() const { return get_ns() / 1e3; }

  /// Return nanoseconds elapsed since this timer was created or last reset
  size_t get_ns() const {
    return static_cast<size_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now() - start_time_)
            .count());
  }

 private:
  std::chrono::time_point<std::chrono::high_resolution_clock> start_time_;
};