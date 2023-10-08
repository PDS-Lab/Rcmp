#pragma once

/**
 * @file dlog.h

 * @brief terminal log output macro

 * @version 0.1
 * @date 2022-05-27
 */

#include <pthread.h>
#include <sys/time.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>

#define DLOG_STREAM(stream, format, ...)                                                  \
    do {                                                                                  \
        struct timeval tv;                                                                \
        struct tm tm;                                                                     \
        char tbuf[28] = {0};                                                              \
        gettimeofday(&tv, NULL);                                                          \
        localtime_r(&tv.tv_sec, &tm);                                                     \
        strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", &tm);                           \
        fprintf(stream, "[%s.%06d] [%d %#lx] %s:%d: " format "\n", tbuf, (int)tv.tv_usec, \
                getpid(), pthread_self(), __FILE__, __LINE__, ##__VA_ARGS__);             \
    } while (0)

#define DLOG_INFO(format, ...) DLOG_STREAM(stderr, "[INFO] " format, ##__VA_ARGS__)
#define DLOG_ERROR(format, ...) \
    DLOG_STREAM(stderr, "[ERROR] " format ": %s", ##__VA_ARGS__, strerror(errno))
#define DLOG_WARNING(format, ...) DLOG_STREAM(stderr, "[WARNING] " format, ##__VA_ARGS__)
#define DLOG_FATAL(format, ...)                                                        \
    do {                                                                               \
        DLOG_STREAM(stderr, "[FATAL] " format ": %s", ##__VA_ARGS__, strerror(errno)); \
        fflush(stdout);                                                                \
        abort();                                                                       \
    } while (0)

#define DLOG(format, ...) DLOG_INFO(format, ##__VA_ARGS__)

#define DLOG_FILE(file, format, ...)            \
    do {                                        \
        FILE *fp = fopen(file, "w+");           \
        assert(fp != NULL);                     \
        DLOG_STREAM(fp, format, ##__VA_ARGS__); \
        fclose(fp);                             \
    } while (0)

#define DLOG_IF(expr, format, ...)             \
    do {                                       \
        if (expr) DLOG(format, ##__VA_ARGS__); \
    } while (0)

#ifndef NDEBUG

namespace type_fmt_str_detail {
template <typename T>
struct helper;
template <>
struct helper<int> {
    constexpr static const char *type_str = "%d";
};
template <>
struct helper<unsigned int> {
    constexpr static const char *type_str = "%u";
};
template <>
struct helper<char> {
    constexpr static const char *type_str = "%c";
};
template <>
struct helper<unsigned char> {
    constexpr static const char *type_str = "%hhu";
};
template <>
struct helper<short> {
    constexpr static const char *type_str = "%hd";
};
template <>
struct helper<unsigned short> {
    constexpr static const char *type_str = "%hu";
};
template <>
struct helper<long> {
    constexpr static const char *type_str = "%ld";
};
template <>
struct helper<unsigned long> {
    constexpr static const char *type_str = "%lu";
};
template <>
struct helper<long long> {
    constexpr static const char *type_str = "%lld";
};
template <>
struct helper<unsigned long long> {
    constexpr static const char *type_str = "%llu";
};
template <>
struct helper<float> {
    constexpr static const char *type_str = "%f";
};
template <>
struct helper<double> {
    constexpr static const char *type_str = "%lf";
};
template <>
struct helper<long double> {
    constexpr static const char *type_str = "%llf";
};
template <typename T>
struct helper<T *> {
    constexpr static const char *type_str = "%p";
};
template <>
struct helper<char *> {
    constexpr static const char *type_str = "%s";
};
template <>
struct helper<const char *> {
    constexpr static const char *type_str = "%s";
};
}  // namespace type_fmt_str_detail

/**
 * Assert the judgment between two values.
 * @example DLOG_EXPR(malloc(1), !=, nullptr)
 *
 * @warning In C++11, `NULL` will throw warning: passing NULL to non-pointer
 * argument... You should use `nullptr` instead of `NULL`.
 */
#define DLOG_EXPR(val_a, op, val_b)                                                                \
    do {                                                                                           \
        decltype(val_a) a = val_a;                                                                 \
        decltype(val_b) b = val_b;                                                                 \
        if (__glibc_unlikely(!(a op b))) {                                                         \
            char fmt[] = "Because " #val_a " = %???, " #val_b " = %???";                           \
            char tmp[sizeof(fmt) + 42];                                                            \
            snprintf(fmt, sizeof(fmt), "Because " #val_a " = %s, " #val_b " = %s",                 \
                     type_fmt_str_detail::helper<typename std::remove_cv<                          \
                         typename std::remove_reference<decltype(val_a)>::type>::type>::type_str,  \
                     type_fmt_str_detail::helper<typename std::remove_cv<                          \
                         typename std::remove_reference<decltype(val_b)>::type>::type>::type_str); \
            snprintf(tmp, sizeof(tmp), fmt, a, b);                                                 \
            DLOG_FATAL("Assertion `" #val_a " " #op " " #val_b "` failed. %s", tmp);               \
        }                                                                                          \
    } while (0)

#define DLOG_ASSERT(expr, format...)                             \
    do {                                                         \
        if (__glibc_unlikely(!(expr))) {                         \
            DLOG_FATAL("Assertion `" #expr "` failed. " format); \
        }                                                        \
    } while (0)

#else

#define DLOG_EXPR(val_a, op, val_b) \
    do {                            \
    } while (0)
#define DLOG_ASSERT(expr, format...) \
    do {                             \
    } while (0)

#endif