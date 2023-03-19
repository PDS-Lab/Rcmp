#include "lock.hpp"

#include <emmintrin.h>

SpinMutex::SpinMutex() { pthread_spin_init(&m_spinlock, 0); }
SpinMutex::~SpinMutex() { pthread_spin_destroy(&m_spinlock); }
void SpinMutex::lock() { pthread_spin_lock(&m_spinlock); }
bool SpinMutex::try_lock() { return pthread_spin_trylock(&m_spinlock) == 0; }
void SpinMutex::unlock() { pthread_spin_unlock(&m_spinlock); }

TinySpinMutex::TinySpinMutex() : m_flag(0) {}
void TinySpinMutex::lock() {
    while (m_flag.test_and_set(std::memory_order_acquire)) {
        _mm_pause();
    }
}
bool TinySpinMutex::try_lock() { return !m_flag.test_and_set(std::memory_order_acquire); }
void TinySpinMutex::unlock() { m_flag.clear(std::memory_order_release); }

SharedMutex::SharedMutex() { pthread_rwlock_init(&m_rwlock, NULL); }
SharedMutex::~SharedMutex() { pthread_rwlock_destroy(&m_rwlock); }
void SharedMutex::lock() { pthread_rwlock_wrlock(&m_rwlock); }
bool SharedMutex::try_lock() { return pthread_rwlock_trywrlock(&m_rwlock) == 0; }
void SharedMutex::unlock() { pthread_rwlock_unlock(&m_rwlock); }
void SharedMutex::lock_shared() { pthread_rwlock_rdlock(&m_rwlock); }
bool SharedMutex::try_lock_shared() { return pthread_rwlock_tryrdlock(&m_rwlock) == 0; }
void SharedMutex::unlock_shared() { pthread_rwlock_unlock(&m_rwlock); }

SharedLockGuard::SharedLockGuard(SharedMutex& mutex, bool isWriteLock) : m_mutex(mutex) {
    if (isWriteLock) {
        m_mutex.lock();
    } else {
        m_mutex.lock_shared();
    }
}
SharedLockGuard::~SharedLockGuard() { m_mutex.unlock(); }