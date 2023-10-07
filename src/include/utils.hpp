#pragma once

#include <unistd.h>

#include <atomic>
#include <functional>
#include <future>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#define CACHE_ALIGN __attribute__((aligned(cache_line_size)))

#define LIKELY __glibc_likely
#define UNLIKELY __glibc_unlikely

#ifdef NDEBUG
#define DEBUGY(cond) if (false)
#else
#define DEBUGY(cond) if (UNLIKELY(cond))
#endif  // NDEBUG

template <typename T>
using MaxHeap = std::priority_queue<T, std::vector<T>, std::less<T>>;

template <typename T>
using MinHeap = std::priority_queue<T, std::vector<T>, std::greater<T>>;

template <typename D>
constexpr D div_ceil(D x, uint64_t div) {
    return (x + div - 1) / div;
}

template <typename D>
D div_floor(D x, uint64_t div) {
    return x / div;
}

template <typename D>
D align_ceil(D x, uint64_t aligned) {
    return div_ceil(x, aligned) * aligned;
}

template <typename D>
D align_floor(D x, uint64_t aligned) {
    return div_floor(x, aligned) * aligned;
}

inline uint64_t rdtsc() { return __builtin_ia32_rdtsc(); }

void threadBindCore(int core_id);
uint64_t getMsTimestamp();
uint64_t getUsTimestamp();
uint64_t getNsTimestamp();

class IPv4String {
   public:
    IPv4String() = default;
    IPv4String(const std::string &ip);
    IPv4String(const IPv4String &ip) = default;
    IPv4String(IPv4String &&ip) = default;
    IPv4String &operator=(const std::string &ip);
    IPv4String &operator=(const IPv4String &ip) = default;
    IPv4String &operator=(IPv4String &&ip) = default;

    std::string get_string() const { return std::string(raw.ipstr); }

   private:
    struct {
        char ipstr[16];
    } raw;
};

struct NOCOPYABLE {
    NOCOPYABLE() = default;
    ~NOCOPYABLE() = default;
    NOCOPYABLE(const NOCOPYABLE &) = delete;
    NOCOPYABLE(NOCOPYABLE &&) = delete;
    NOCOPYABLE &operator=(const NOCOPYABLE &) = delete;
    NOCOPYABLE &operator=(NOCOPYABLE &&) = delete;
};

template <typename R, typename... Args>
struct function_traits_helper {
    static constexpr std::size_t count = sizeof...(Args);
    using result_type = R;
    using args_tuple_type = std::tuple<Args...>;
    template <std::size_t N>
    using args_type = typename std::tuple_element<N, std::tuple<Args...>>::type;
};

template <typename T>
struct function_traits;
template <typename R, typename... Args>
struct function_traits<R(Args...)> : public function_traits_helper<R, Args...> {};
template <typename R, typename... Args>
struct function_traits<R (*)(Args...)> : public function_traits_helper<R, Args...> {};
template <typename R, typename... Args>
struct function_traits<R (&)(Args...)> : public function_traits_helper<R, Args...> {};

template <typename T>
struct container_traits;

template <template <typename T> class C, typename T>
struct container_traits<C<T>> {
    using type = T;

    template <typename R>
    using recontain_type = C<R>;
};

struct atomic_po_val_t {
    union {
        struct {
            uint32_t pos;
            uint32_t cnt;
        };
        uint64_t raw;
    };

    atomic_po_val_t load(std::memory_order __m = std::memory_order_seq_cst) const {
        atomic_po_val_t o;
        o.raw = __atomic_load_n(&raw, (int)__m);
        return o;
    }

    uint32_t fetch_add_cnt(uint32_t cnt_, std::memory_order __m = std::memory_order_seq_cst) {
        return __atomic_fetch_add(&this->cnt, cnt_, (int)__m);
    }

    uint32_t fetch_add_pos(uint32_t pos_, std::memory_order __m = std::memory_order_seq_cst) {
        return __atomic_fetch_add(&this->pos, pos_, (int)__m);
    }

    atomic_po_val_t fetch_add_both(uint32_t pos, uint32_t cnt,
                                   std::memory_order __m = std::memory_order_seq_cst) {
        atomic_po_val_t o;
        o.cnt = cnt;
        o.pos = pos;
        o.raw = __atomic_fetch_add(&raw, o.raw, (int)__m);
        return o;
    }

    bool compare_exchange_weak(atomic_po_val_t &expected, atomic_po_val_t desired,
                               std::memory_order __s = std::memory_order_seq_cst,
                               std::memory_order __f = std::memory_order_seq_cst) {
        return __atomic_compare_exchange_n(&raw, &expected.raw, desired.raw, true, (int)__s,
                                           (int)__f);
    }
};

inline void __DEBUG_START_PERF() {
    if (system(("sudo perf record -F 99 -g -p " + std::to_string(getpid()) + " &").c_str())) {
    }
}

inline void __RANDOM_SLEEP() { usleep(rand() % 10); }
