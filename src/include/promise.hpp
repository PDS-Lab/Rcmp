#pragma once

#include <atomic>
#include <boost/fiber/future/future.hpp>
#include <boost/fiber/future/promise.hpp>
#include <future>
#include <stdexcept>
#include <thread>

template <typename T>
class SpinFuture;

template <typename T>
class SpinPromise {
   public:
    SpinPromise() : ready_(false) {}
    ~SpinPromise() {}

    SpinFuture<T> get_future() { return SpinFuture<T>(this); }

    void set_value(const T &value) {
        value_ = value;
        ready_.store(true, std::memory_order_release);
    }

   private:
    friend class SpinFuture<T>;

    T value_;
    std::atomic_bool ready_;
};

template <typename T>
class SpinFuture {
   public:
    SpinFuture(SpinPromise<T> *promise) : promise_(promise) {}
    ~SpinFuture() {}

    const T &get() const {
        while (!promise_->ready_.load(std::memory_order_acquire)) {
            // spin
        }
        return promise_->value_;
    }

    template <typename _Rep, typename _Period>
    std::future_status wait_for(const std::chrono::duration<_Rep, _Period> &__rel) const {
        if (promise_->ready_.load(std::memory_order_acquire)) return std::future_status::ready;
        if (__rel > __rel.zero()) {
            std::this_thread::sleep_for(__rel);
            if (promise_->ready_.load(std::memory_order_acquire)) return std::future_status::ready;
        }
        return std::future_status::timeout;
    }

   private:
    SpinPromise<T> *promise_;
};

template <>
class SpinPromise<void>;
template <>
class SpinFuture<void>;

template <>
class SpinPromise<void> {
   public:
    SpinPromise() : ready_(false) {}
    ~SpinPromise() {}

    SpinFuture<void> get_future();

    void set_value() { ready_.store(true, std::memory_order_release); }

   private:
    friend class SpinFuture<void>;

    std::atomic_bool ready_;
};

template <>
class SpinFuture<void> {
   public:
    SpinFuture(SpinPromise<void> *promise) : promise_(promise) {}
    ~SpinFuture() {}

    void get() const {
        while (!promise_->ready_.load(std::memory_order_acquire)) {
            // spin
        }
    }

    void wait() { throw std::runtime_error("wait dead spinning"); }

    template <typename _Rep, typename _Period>
    std::future_status wait_for(const std::chrono::duration<_Rep, _Period> &__rel) const {
        if (promise_->ready_.load(std::memory_order_acquire)) return std::future_status::ready;
        if (__rel > __rel.zero()) {
            std::this_thread::sleep_for(__rel);
            if (promise_->ready_.load(std::memory_order_acquire)) return std::future_status::ready;
        }
        return std::future_status::timeout;
    }

   private:
    SpinPromise<void> *promise_;
};

inline SpinFuture<void> SpinPromise<void>::get_future() { return SpinFuture<void>(this); }

template <typename T>
using CortPromise = boost::fibers::promise<T>;
template <typename T>
using CortFuture = boost::fibers::future<T>;

struct FutureControlBlock {
    bool ready = false;
    boost::fibers::mutex mtx;
    boost::fibers::condition_variable cv;

    void clear() { ready = false; }

    void get() {
        std::unique_lock<boost::fibers::mutex> lck(mtx);
        cv.wait(lck, [&]() { return ready; });
    }
    void set_value() {
        {
            std::unique_lock<boost::fibers::mutex> lck(mtx);
            ready = true;
        }
        cv.notify_all();
    }
};