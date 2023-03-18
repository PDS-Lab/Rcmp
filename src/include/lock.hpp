#pragma once

#include <atomic>
#include <mutex>
#include <shared_mutex>

class Mutex : public std::mutex {};

class SpinMutex {
    public:
    SpinMutex();
};

class TinySpinMutex {};

class SharedMutex {};

class SharedLockGuard {};
