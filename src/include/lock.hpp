#pragma once

#include <pthread.h>

#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/mutex.hpp>
#include <mutex>
#include <shared_mutex>

using Mutex = std::mutex;
using SharedMutex = std::shared_mutex;

class SpinMutex {
   public:
    SpinMutex() { pthread_spin_init(&m_spinlock, 0); }
    ~SpinMutex() { pthread_spin_destroy(&m_spinlock); }

    void lock() { pthread_spin_lock(&m_spinlock); }
    bool try_lock() { return pthread_spin_trylock(&m_spinlock) == 0; }
    void unlock() { pthread_spin_unlock(&m_spinlock); }

   private:
    pthread_spinlock_t m_spinlock;
};

class Barrier {
   public:
    Barrier(uint32_t n) { pthread_barrier_init(&m_b, nullptr, n); }
    ~Barrier() { pthread_barrier_destroy(&m_b); }

    void wait() { pthread_barrier_wait(&m_b); }

   private:
    pthread_barrier_t m_b;
};

using CortMutex = boost::fibers::mutex;
using CortConditionalVariable = boost::fibers::condition_variable;

class CortSharedMutex {
   public:
    CortSharedMutex() : state(0) {}

    void lock() {
        std::unique_lock<boost::fibers::mutex> lk(mtx);
        g1.wait(lk, [=] { return !write_entered(); });
        state |= _S_write_entered;
        g2.wait(lk, [=] { return readers() == 0; });
    }

    bool try_lock() {
        std::unique_lock<boost::fibers::mutex> lk(mtx, std::try_to_lock);
        if (lk.owns_lock() && state == 0) {
            state = _S_write_entered;
            return true;
        }
        return false;
    }

    void unlock() {
        std::lock_guard<boost::fibers::mutex> lk(mtx);
        state = 0;
        g1.notify_all();
    }

    void lock_shared() {
        std::unique_lock<boost::fibers::mutex> lk(mtx);
        g1.wait(lk, [=] { return state < _S_max_readers; });
        ++state;
    }

    bool try_lock_shared() {
        std::unique_lock<boost::fibers::mutex> lk(mtx, std::try_to_lock);
        if (!lk.owns_lock()) {
            return false;
        }
        if (state < _S_max_readers) {
            ++state;
            return true;
        }
        return false;
    }

    void unlock_shared() {
        std::lock_guard<boost::fibers::mutex> lk(mtx);
        auto prev = state--;
        if (write_entered()) {
            if (readers() == 0) {
                g2.notify_one();
            }
        } else {
            if (prev == _S_max_readers) {
                g1.notify_one();
            }
        }
    }

   private:
    boost::fibers::mutex mtx;
    boost::fibers::condition_variable g1, g2;
    unsigned state;

    static constexpr unsigned _S_write_entered = 1U << (sizeof(unsigned) * __CHAR_BIT__ - 1);
    static constexpr unsigned _S_max_readers = ~_S_write_entered;

    bool write_entered() const { return state & _S_write_entered; }
    unsigned readers() const { return state & _S_max_readers; }
};