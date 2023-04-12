#pragma once

#include <cstddef>
#include <unordered_map>
#include <vector>

class LRUCache {};

template <typename K, typename V>
class ClockLRU {
    struct CacheEntry {
        K key;
        V value;
        bool ref;
        size_t refs;
    };

   public:
    class Handle {
        friend class ClockLRU<K, V>;

       public:
        bool valid() const { return lru != nullptr && iter != lru->cache_.end(); }
        const K& key() const { return iter->key; }
        V& operator*() { return iter->value; }
        V* operator->() { return &(iter->value); }

        Handle() = default;
        Handle(const Handle& h) : lru(h.lru), iter(h.iter) { Ref(); }
        Handle(Handle&& h) : lru(h.lru), iter(h.iter) { Ref(); }
        Handle& operator=(const Handle& h) {
            lru = h.lru;
            iter = h.iter;
            Ref();
        }
        Handle& operator=(Handle&& h) {
            lru = h.lru;
            iter = h.iter;
            Ref();
        }
        ~Handle() { UnRef(); }

       private:
        ClockLRU* lru;
        typename std::vector<CacheEntry>::iterator iter;

        void Ref() {
            if (valid()) {
                ++iter->refs;
            }
        }
        void UnRef() {
            if (valid()) {
                --iter->refs;
            }
        }
    };

    ClockLRU(size_t capacity) : capacity_(capacity), clock_hand_(0) {}

    Handle get(K key) {
        Handle h;
        auto it = cache_map_.find(key);
        if (it == cache_map_.end()) {
            h.lru = nullptr;
            return h;
        }
        it->second->ref = true;
        h.lru = &cache_;
        h.iter = it->second;
        ++it->second->refs;
        return h;
    }

    template <typename Deleter>
    void put(K key, V value, Deleter del) {
        auto it = cache_map_.find(key);
        if (it != cache_map_.end()) {
            it->second->value = value;
            it->second->ref = true;
            return;
        }
        if (cache_.size() < capacity_) {
            cache_.push_back({key, value, true});
            cache_map_[key] = --cache_.end();
            return;
        }
        while (cache_[clock_hand_].ref || cache_[clock_hand_].refs > 0) {
            cache_[clock_hand_].ref = false;
            clock_hand_ = (clock_hand_ + 1) % capacity_;
        }

        {
            Handle del_h = get(cache_[clock_hand_].key);
            del(del_h);
        }

        // TODO: 利用cache_map_的iter删除减少查询
        cache_map_.erase(cache_[clock_hand_].key);
        cache_[clock_hand_] = {key, value, true};
        cache_map_[key] = cache_.begin() + clock_hand_;
        clock_hand_ = (clock_hand_ + 1) % capacity_;
    }

   private:
    std::unordered_map<K, typename std::vector<CacheEntry>::iterator> cache_map_;
    std::vector<CacheEntry> cache_;
    size_t capacity_;
    size_t clock_hand_;
};
