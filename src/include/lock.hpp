#pragma once

#include <pthread.h>

#include <atomic>
#include <mutex>

class Mutex : public std::mutex {};

class SpinMutex {
   public:
    SpinMutex();
    ~SpinMutex();

    void lock();
    bool try_lock();
    void unlock();

   private:
    pthread_spinlock_t m_spinlock;
};

class TinySpinMutex {
   public:
    TinySpinMutex();

    void lock();
    bool try_lock();
    void unlock();

   private:
    std::atomic_flag m_flag;
};

class SharedMutex {
   public:
    SharedMutex();
    ~SharedMutex();

    void lock();
    bool try_lock();
    void unlock();
    void lock_shared();
    bool try_lock_shared();
    void unlock_shared();

   private:
    pthread_rwlock_t m_rwlock;
};

template <typename __SharedMutex>
class SharedLockGuard {
   public:
    SharedLockGuard(__SharedMutex& mutex, bool isWriteLock) : m_mutex(mutex), m_is_write(isWriteLock) {
        if (m_is_write) {
            m_mutex.lock();
        } else {
            m_mutex.lock_shared();
        }
    }

    ~SharedLockGuard() {
        if (m_is_write) {
            m_mutex.unlock();
        } else {
            m_mutex.unlock_shared();
        }
    }

   private:
    bool m_is_write;
    __SharedMutex& m_mutex;
};

class SharedCortMutex {
   public:
    SharedCortMutex();
    ~SharedCortMutex();

    void lock();
    void unlock();
    void lock_shared();
    void unlock_shared();

   private:
    std::atomic<uint32_t> m_rw_cnt;
};