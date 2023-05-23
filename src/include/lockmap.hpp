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
    void lock(ID id) {
        auto &shard = m_shards[hash(id)];

    retry:
        std::shared_lock<__ShreadMutex> lock(shard.mutex_);
        auto it = shard.locks_.find(id);
        if (it == shard.locks_.end()) {
            lock.unlock();
            std::unique_lock<__ShreadMutex> lock(shard.mutex_);
            if (shard.locks_.find(id) == shard.locks_.end()) {
                shard.locks_.insert({id, std::make_unique<__ShreadMutex>()});
            }
            goto retry;
        }
        it->second->lock();
    }

    bool try_lock(ID id) {
        auto &shard = m_shards[hash(id)];

    retry:
        std::shared_lock<__ShreadMutex> lock(shard.mutex_);
        auto it = shard.locks_.find(id);
        if (it == shard.locks_.end()) {
            lock.unlock();
            std::unique_lock<__ShreadMutex> lock(shard.mutex_);
            if (shard.locks_.find(id) == shard.locks_.end()) {
                shard.locks_.insert({id, std::make_unique<__ShreadMutex>()});
            }
            goto retry;
        }
        return it->second->try_lock();
    }

    void unlock(ID id) {
        auto &shard = m_shards[hash(id)];

        std::shared_lock<__ShreadMutex> lock(shard.mutex_);
        auto it = shard.locks_.find(id);
        it->second->unlock();

        if (it->second->try_lock()) {
            lock.unlock();
            std::unique_lock<__ShreadMutex> lock(shard.mutex_, std::try_to_lock);
            it->second->unlock();
            if (lock.owns_lock()) {
                shard.locks_.erase(it);
            }
        }
    }

    void lock_shared(ID id) {
        auto &shard = m_shards[hash(id)];

    retry:
        std::shared_lock<__ShreadMutex> lock(shard.mutex_);
        auto it = shard.locks_.find(id);
        if (it == shard.locks_.end()) {
            lock.unlock();
            std::unique_lock<__ShreadMutex> lock(shard.mutex_);
            if (shard.locks_.find(id) == shard.locks_.end()) {
                shard.locks_.insert({id, std::make_unique<__ShreadMutex>()});
            }
            goto retry;
        }
        it->second->lock_shared();
    }

    bool try_lock_shared(ID id) {
        auto &shard = m_shards[hash(id)];

    retry:
        std::shared_lock<__ShreadMutex> lock(shard.mutex_);
        auto it = shard.locks_.find(id);
        if (it == shard.locks_.end()) {
            lock.unlock();
            std::unique_lock<__ShreadMutex> lock(shard.mutex_);
            if (shard.locks_.find(id) == shard.locks_.end()) {
                shard.locks_.insert({id, std::make_unique<__ShreadMutex>()});
            }
            goto retry;
        }
        return it->second->try_lock_shared();
    }

    void unlock_shared(ID id) {
        auto &shard = m_shards[hash(id)];

        std::shared_lock<__ShreadMutex> lock(shard.mutex_);
        auto it = shard.locks_.find(id);
        it->second->unlock_shared();

        if (it->second->try_lock()) {
            lock.unlock();
            std::unique_lock<__ShreadMutex> lock(shard.mutex_, std::try_to_lock);
            it->second->unlock();
            if (lock.owns_lock()) {
                shard.locks_.erase(it);
            }
        }

        /**
         * ! 当一个线程没有try_lock成功，shared_lock依旧持有，
         *   另一个线程try_to_lock则失败，导致锁未被及时erase
         */
    }

   private:
    constexpr static const size_t BucketNum = 32;

    struct Shard {
        __ShreadMutex mutex_;
        std::unordered_map<ID, std::unique_ptr<__ShreadMutex>> locks_;
    };

    Shard m_shards[BucketNum];

    static size_t hash(ID key) { return std::hash<ID>()(key) % BucketNum; }
};

template <typename ID, typename __LockResourceManager>
class UniqueResourceLock {
   public:
    UniqueResourceLock() : m_pm(nullptr), m_owns(false) {}

    UniqueResourceLock(__LockResourceManager &lr, ID id)
        : m_pm(std::addressof(lr)), m_owns(true), m_id(id) {
        lr.lock(id);
    }

    UniqueResourceLock(__LockResourceManager &lr, ID id, std::defer_lock_t)
        : m_pm(std::addressof(lr)), m_owns(false), m_id(id) {}

    UniqueResourceLock(__LockResourceManager &lr, ID id, std::try_to_lock_t)
        : m_pm(std::addressof(lr)), m_owns(lr.try_lock()), m_id(id) {}

    UniqueResourceLock(__LockResourceManager &lr, ID id, std::adopt_lock_t)
        : m_pm(std::addressof(lr)), m_owns(true), m_id(id) {}

    ~UniqueResourceLock() {
        if (m_owns) {
            m_pm->unlock(m_id);
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
        m_pm->lock();
        m_owns = true;
    }

    bool try_lock() {
        lockable();
        return m_owns = m_pm->lock();
    }

    void unlock() {
        if (!m_owns) std::__throw_system_error(int(std::errc::resource_deadlock_would_occur));
        m_pm->unlock();
        m_owns = false;
    }

    void swap(UniqueResourceLock &ul) {
        std::swap(m_pm, ul.m_pm);
        std::swap(m_owns, ul.m_owns);
        std::swap(m_id, ul.m_id);
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
    ID m_id;
    bool m_owns;
};

template <typename ID, typename __LockResourceManager>
class SharedResourceLock {
   public:
    SharedResourceLock() : m_pm(nullptr), m_owns(false) {}

    SharedResourceLock(__LockResourceManager &lr, ID id)
        : m_pm(std::addressof(lr)), m_owns(true), m_id(id) {
        lr.lock_shared(id);
    }

    SharedResourceLock(__LockResourceManager &lr, ID id, std::defer_lock_t)
        : m_pm(std::addressof(lr)), m_owns(false), m_id(id) {}

    SharedResourceLock(__LockResourceManager &lr, ID id, std::try_to_lock_t)
        : m_pm(std::addressof(lr)), m_owns(lr.try_lock_shared()), m_id(id) {}

    SharedResourceLock(__LockResourceManager &lr, ID id, std::adopt_lock_t)
        : m_pm(std::addressof(lr)), m_owns(true), m_id(id) {}

    ~SharedResourceLock() {
        if (m_owns) {
            m_pm->unlock_shared(m_id);
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
        m_pm->lock_shared();
        m_owns = true;
    }

    bool try_lock() {
        lockable();
        return m_owns = m_pm->lock_shared();
    }

    void unlock() {
        if (!m_owns) std::__throw_system_error(int(std::errc::resource_deadlock_would_occur));
        m_pm->unlock_shared();
        m_owns = false;
    }

    void swap(SharedResourceLock &ul) {
        std::swap(m_pm, ul.m_pm);
        std::swap(m_owns, ul.m_owns);
        std::swap(m_id, ul.m_id);
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
    ID m_id;
    bool m_owns;
};