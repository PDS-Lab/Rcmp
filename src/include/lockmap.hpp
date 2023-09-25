#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "lock.hpp"

/**
 * @brief 锁资源索引
 *
 * @warning lock_shared可能会出现资源泄露
 *
 * @tparam ID
 * @tparam __ShreadMutex
 */
template <typename ID, typename __ShreadMutex = SharedMutex>
class LockResourceManager {
   public:
    using lock_type = __ShreadMutex;

    __ShreadMutex *get_lock(ID id) {
        auto &shard = m_shards[hash(id)];

    retry:
        std::shared_lock<__ShreadMutex> lock(shard.mutex_);
        auto it = shard.locks_.find(id);
        if (it == shard.locks_.end()) {
            lock.unlock();
            std::unique_lock<__ShreadMutex> lock(shard.mutex_);
            if (shard.locks_.find(id) == shard.locks_.end()) {
                shard.locks_.insert({id, new __ShreadMutex()});
            }
            goto retry;
        }
        return it->second;
    }

    // TODO: auto release lock resource

   private:
    constexpr static const size_t BucketNum = 32;

    struct Shard {
        __ShreadMutex mutex_;
        std::unordered_map<ID, __ShreadMutex *> locks_;
    };

    Shard m_shards[BucketNum];

    static size_t hash(ID key) { return std::hash<ID>()(key) % BucketNum; }
};

template <typename ID, typename __LockResourceManager>
class UniqueResourceLock {
   public:
    using lock_type = typename __LockResourceManager::lock_type;

    UniqueResourceLock() : m_pm(nullptr), m_owns(false), m_lck(nullptr) {}

    UniqueResourceLock(__LockResourceManager &lr, ID id)
        : m_pm(std::addressof(lr)), m_owns(true), m_lck(lr.get_lock(id)) {
        m_lck->lock();
    }

    UniqueResourceLock(__LockResourceManager &lr, ID id, std::defer_lock_t)
        : m_pm(std::addressof(lr)), m_owns(false), m_lck(lr.get_lock(id)) {}

    UniqueResourceLock(__LockResourceManager &lr, ID id, std::try_to_lock_t)
        : m_pm(std::addressof(lr)), m_lck(lr.get_lock(id)), m_owns(m_lck->try_lock()) {}

    UniqueResourceLock(__LockResourceManager &lr, ID id, std::adopt_lock_t)
        : m_pm(std::addressof(lr)), m_owns(true), m_lck(lr.get_lock(id)) {}

    ~UniqueResourceLock() {
        if (m_owns) {
            m_lck->unlock();
        }
    }

    UniqueResourceLock(UniqueResourceLock const &) = delete;
    UniqueResourceLock &operator=(UniqueResourceLock const &) = delete;

    UniqueResourceLock(UniqueResourceLock &&ul) { swap(ul); }

    UniqueResourceLock &operator=(UniqueResourceLock &&ul) {
        swap(ul);
        return *this;
    }

    void lock() {
        lockable();
        m_lck->lock();
        m_owns = true;
    }

    bool try_lock() {
        lockable();
        return m_owns = m_lck->try_lock();
    }

    void unlock() {
        if (!m_owns) std::__throw_system_error(int(std::errc::resource_deadlock_would_occur));
        m_lck->unlock();
        m_owns = false;
    }

    void swap(UniqueResourceLock &ul) {
        std::swap(m_pm, ul.m_pm);
        std::swap(m_owns, ul.m_owns);
        std::swap(m_lck, ul.m_lck);
    }

    bool owns_lock() const { return m_owns; }

    operator bool() const { return m_owns; }

    __LockResourceManager *release() {
        m_owns = false;
        return std::exchange(m_pm, nullptr);
    }

   private:
    void lockable() const {
        if (m_pm == nullptr) std::__throw_system_error(int(std::errc::operation_not_permitted));
        if (m_owns) std::__throw_system_error(int(std::errc::resource_deadlock_would_occur));
    }

    __LockResourceManager *m_pm;
    lock_type *m_lck;
    bool m_owns;
};

template <typename ID, typename __LockResourceManager>
class SharedResourceLock {
   public:
    using lock_type = typename __LockResourceManager::lock_type;

    SharedResourceLock() : m_pm(nullptr), m_owns(false), m_lck(nullptr) {}

    SharedResourceLock(__LockResourceManager &lr, ID id)
        : m_pm(std::addressof(lr)), m_owns(true), m_lck(lr.get_lock(id)) {
        m_lck->lock_shared();
    }

    SharedResourceLock(__LockResourceManager &lr, ID id, std::defer_lock_t)
        : m_pm(std::addressof(lr)), m_owns(false), m_lck(lr.get_lock(id)) {}

    SharedResourceLock(__LockResourceManager &lr, ID id, std::try_to_lock_t)
        : m_pm(std::addressof(lr)), m_lck(lr.get_lock(id)), m_owns(m_lck->try_lock_shared()) {}

    SharedResourceLock(__LockResourceManager &lr, ID id, std::adopt_lock_t)
        : m_pm(std::addressof(lr)), m_owns(true), m_lck(lr.get_lock(id)) {}

    ~SharedResourceLock() {
        if (m_owns) {
            m_lck->unlock_shared();
        }
    }

    SharedResourceLock(SharedResourceLock const &) = delete;
    SharedResourceLock &operator=(SharedResourceLock const &) = delete;

    SharedResourceLock(SharedResourceLock &&ul) { swap(ul); }

    SharedResourceLock &operator=(SharedResourceLock &&ul) {
        SharedResourceLock(std::move(ul)).swap(*this);
        return *this;
    }

    void lock() {
        lockable();
        m_lck->lock_shared();
        m_owns = true;
    }

    bool try_lock() {
        lockable();
        return m_owns = m_lck->try_lock_shared();
    }

    void unlock() {
        if (!m_owns) std::__throw_system_error(int(std::errc::resource_deadlock_would_occur));
        m_lck->unlock_shared();
        m_owns = false;
    }

    void swap(SharedResourceLock &ul) {
        std::swap(m_pm, ul.m_pm);
        std::swap(m_owns, ul.m_owns);
        std::swap(m_lck, ul.m_lck);
    }

    bool owns_lock() const { return m_owns; }

    operator bool() const { return m_owns; }

    __LockResourceManager *release() {
        m_owns = false;
        return std::exchange(m_pm, nullptr);
    }

   private:
    void lockable() const {
        if (m_pm == nullptr) std::__throw_system_error(int(std::errc::operation_not_permitted));
        if (m_owns) std::__throw_system_error(int(std::errc::resource_deadlock_would_occur));
    }

    __LockResourceManager *m_pm;
    lock_type *m_lck;
    bool m_owns;
};