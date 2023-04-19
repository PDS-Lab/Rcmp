#pragma once

#include <unordered_map>

#include "config.hpp"
#include "lock.hpp"
#include "utils.hpp"

template <typename K, typename V>
class ConcurrentHashMap {
    constexpr static const size_t BucketNum = 32;

   public:
    ConcurrentHashMap() = default;
    ~ConcurrentHashMap() = default;

    bool insert(K key, V val) {
        int index = hash(key);
        auto& shard = m_shards[index];

        SharedLockGuard guard(shard.m_lock, true);
        auto& map = shard.m_map;
        auto it = map.find(key);
        if (it == map.end()) {
            map.insert({key, val});
            return true;
        }
        return false;
    }

    bool find(K key, V* val) {
        int index = hash(key);
        auto& shard = m_shards[index];

        SharedLockGuard guard(shard.m_lock, false);
        auto& map = shard.m_map;
        auto it = map.find(key);
        if (it != map.end()) {
            *val = it->second;
            return true;
        }
        return false;
    }

    bool erase(K key) {
        int index = hash(key);
        auto& shard = m_shards[index];

        SharedLockGuard guard(shard.m_lock, true);
        auto& map = shard.m_map;
        auto it = map.find(key);
        if (it != map.end()) {
            map.erase(it);
            return true;
        }
        return false;
    }

    bool empty() const {
        for (int i = 0; i < BucketNum; ++i) {
            if (!m_shards[i].m_map.empty()) {
                return false;
            }
        }
        return true;
    }

    int size() const {
        int count = 0;
        for (int i = 0; i < BucketNum; ++i) {
            count += m_shards[i].m_map.size();
        }
        return count;
    }

   private:
    struct Shard {
        CACHE_ALIGN SharedMutex m_lock;
        std::unordered_map<K, V> m_map;
    };

    Shard m_shards[BucketNum];

    static size_t hash(K key) { return std::hash<K>()(key) % BucketNum; }
};