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

class SharedLockGuard {
   public:
    // 构造函数
    SharedLockGuard(SharedMutex& mutex, bool isWriteLock);

    // 解锁析构函数
    ~SharedLockGuard();

   private:
    SharedMutex& m_mutex;
};
