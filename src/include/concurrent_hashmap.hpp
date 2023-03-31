#pragma once

#include <unordered_map>

#include "config.hpp"
#include "lock.hpp"
#include "utils.hpp"

template <typename K, typename V>
class ConcurrentHashMap {
    constexpr static const size_t BucketNum = 32;

   public:
    ConcurrentHashMap() {
        for (int i = 0; i < BucketNum; ++i) {
            m_maps[i] = std::unordered_map<K, V>();
        }
    }

    bool insert(K key, V val) {
        int index = hash(key);
        SharedLockGuard guard(m_locks[index], true);
        auto& map = m_maps[index];
        auto it = map.find(key);
        if (it == map.end()) {
            map.insert({key, val});
            return true;
        }
        return false;
    }

    bool find(K key, V* val) {
        int index = hash(key);
        SharedLockGuard guard(m_locks[index], false);
        auto& map = m_maps[index];
        auto it = map.find(key);
        if (it != map.end()) {
            *val = it->second;
            return true;
        }
        return false;
    }

    bool erase(K key) {
        int index = hash(key);
        SharedLockGuard guard(m_locks[index], true);
        auto& map = m_maps[index];
        auto it = map.find(key);
        if (it != map.end()) {
            map.erase(it);
            return true;
        }
        return false;
    }

    bool empty() const {
        for (int i = 0; i < BucketNum; ++i) {
            if (!m_maps[i].empty()) {
                return false;
            }
        }
        return true;
    }

    int size() const {
        int count = 0;
        for (int i = 0; i < BucketNum; ++i) {
            count += m_maps[i].size();
        }
        return count;
    }

   private:
    std::unordered_map<K, V> m_maps[BucketNum];
    CACHE_ALIGN SharedMutex m_locks[BucketNum];

    static int hash(K key) { return std::hash<K>()(key) % BucketNum; }
};