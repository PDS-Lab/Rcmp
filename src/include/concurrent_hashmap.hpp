#pragma once

#include <cstddef>

template <typename K, typename V>
class ConcurrentHashMap {
    public:
    bool empty() const;
    size_t size() const;
    bool insert(K key, V val);
    bool find(K key, V *val);
    bool erase(K key);
};