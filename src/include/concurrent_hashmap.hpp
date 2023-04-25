#pragma once

#include <unordered_map>

#include "config.hpp"
#include "lock.hpp"
#include "utils.hpp"

template <typename K, typename V>
class ConcurrentHashMap {
    constexpr static const size_t BucketNum = 32;

    using HashTable = std::unordered_map<K, V>;

   public:
    ConcurrentHashMap() = default;
    ~ConcurrentHashMap() = default;

    /**
     * @brief
     * @warning
     * 重哈希会导致迭代器失效，但无法感知该变化。在大量insert时应该及时更新iterator，或者使用at。
     */
    class iterator {
       public:
        std::pair<const K, V>* operator->() { return it.operator->(); }
        bool operator==(const iterator& other) { return hidx == other.hidx && it == other.it; }
        bool operator!=(const iterator& other) { return hidx != other.hidx || it != other.it; }

       private:
        friend class ConcurrentHashMap;

        iterator(int hidx, typename HashTable::iterator it) : hidx(hidx), it(it) {}

        int hidx;
        typename HashTable::iterator it;
    };

    const iterator end() { return {0, m_shards[0].m_map.end()}; }

    /**
     * @brief 与std::unordered_map::insert相同
     *
     * @param key
     * @param val
     * @return std::pair<iterator, bool>
     */
    std::pair<iterator, bool> insert(K key, V val) {
        int index = hash(key);
        auto& shard = m_shards[index];
        auto& map = shard.m_map;

        SharedLockGuard guard(shard.m_lock, true);
        auto p = map.emplace(key, val);
        return {{index, p.first}, p.second};
    }

    /**
     * @brief 与std::unordered_map::find相同
     *
     * @param key
     * @param val
     * @return std::pair<iterator, bool>
     */
    iterator find(K key) {
        int index = hash(key);
        auto& shard = m_shards[index];
        auto& map = shard.m_map;

        SharedLockGuard guard(shard.m_lock, false);
        auto it = map.find(key);
        if (it != map.end()) {
            return {index, it};
        }
        return end();
    }

    /**
     * @brief 与std::unordered_map::at相同。
     * @warning 未找到时抛出错误
     *
     * @param key
     * @return V&
     */
    V& at(K key) {
        int index = hash(key);
        auto& shard = m_shards[index];
        auto& map = shard.m_map;

        SharedLockGuard guard(shard.m_lock, false);
        return map.at(key);
    }

    /**
     * @brief 查找一个元素。如果不存在，则调用cotr_fn()插入新元素
     *
     * @tparam ConFn
     * @param key
     * @param cotr_fn 返回新元素
     * @return std::pair<iterator, bool> 如果插入成功，返回true；查找成功返回false
     */
    template <typename ConFn>
    std::pair<iterator, bool> find_or_emplace(K key, ConFn&& cotr_fn) {
        int index = hash(key);
        auto& shard = m_shards[index];
        auto& map = shard.m_map;

        do {
            iterator it = find(key);
            if (it != end()) {
                return {it, false};
            }
        } while (!shard.m_lock.try_lock());

        auto p = map.emplace(key, cotr_fn());
        shard.m_lock.unlock();
        return {{index, p.first}, p.second};
    }

    /**
     * @brief 查找一个元素。如果不存在，则调用cotr_fn()尝试插入新元素
     *
     * @tparam ConFn
     * @param key
     * @param try_cotr_fn 返回std::pair<V, bool>，V是新元素，bool是否需要插入新元素
     * @return std::pair<iterator, bool> 如果插入成功，返回true；查找成功、插入失败返回false
     */
    template <typename ConFn>
    std::pair<iterator, bool> find_or_emplace_try(K key, ConFn&& try_cotr_fn) {
        int index = hash(key);
        auto& shard = m_shards[index];
        auto& map = shard.m_map;

        do {
            iterator it = find(key);
            if (it != end()) {
                return {it, false};
            }
        } while (!shard.m_lock.try_lock());

        std::pair<V, bool> v = try_cotr_fn();
        if (v.second) {
            auto p = map.emplace(key, std::move(v.first));
            shard.m_lock.unlock();
            return {{index, p.first}, p.second};
        }
        shard.m_lock.unlock();
        return {end(), false};
    }

    void erase(K key) {
        iterator it = find(key);
        erase(it);
    }

    void erase(iterator it) {
        if (it == end()) return;

        auto& shard = m_shards[it.hidx];
        auto& map = shard.m_map;

        SharedLockGuard guard(shard.m_lock, true);
        map.erase(it.it);
    }

    /**
     * @brief 遍历表
     *
     * @tparam F
     * @param f bool(std::pair<const K, V> &)，返回false代表终止遍历
     */
    template <typename F>
    void foreach_all(F&& f) {
        for (size_t i = 0; i < BucketNum; ++i) {
            auto& shard = m_shards[i];
            auto& map = shard.m_map;

            SharedLockGuard guard(shard.m_lock, false);
            for (auto& p : map) {
                if (!f(p)) {
                    return;
                }
            }
        }
    }

    bool empty() const {
        for (size_t i = 0; i < BucketNum; ++i) {
            if (!m_shards[i].m_map.empty()) {
                return false;
            }
        }
        return true;
    }

    size_t size() const {
        size_t count = 0;
        for (size_t i = 0; i < BucketNum; ++i) {
            count += m_shards[i].m_map.size();
        }
        return count;
    }

   private:
    struct CACHE_ALIGN Shard {
        SharedMutex m_lock;
        HashTable m_map;
    };

    Shard m_shards[BucketNum];

    static size_t hash(K key) { return std::hash<K>()(key) % BucketNum; }
};