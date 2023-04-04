#pragma once

#include <config.hpp>
#include <cstdint>
#include <utility>

#define CACHE_ALIGN __attribute__((aligned(cache_line_size)))

#define LIKELY __glibc_likely
#define UNLIKELY __glibc_unlikely

template <typename D>
D div_ceil(D x, uint64_t div) {
    return (x + div - 1) / div;
}

template <typename D>
D align_by(D x, uint64_t aligned) {
    return div_ceil(x, aligned) * aligned;
}

template <typename D>
D align_floor_by(D x, uint64_t aligned) {
    return (x / aligned) * aligned;
}

void threadBindCore(int core_id);
uint64_t rdtsc();
uint64_t getTimestamp();

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