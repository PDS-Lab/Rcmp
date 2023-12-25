#pragma once

#include <random>
#include <unordered_map>

#include "config.hpp"
#include "lock.hpp"
#include "robin_hood.h"
#include "utils.hpp"

template <typename K, typename V, typename __SharedMutex = SharedMutex, size_t BUCKET_NUM = 32,
          typename _Hash = std::hash<K>>
class ConcurrentHashMap {
    constexpr static const size_t BucketNum = BUCKET_NUM;

    struct SliceHash {
        size_t operator()(K key) const { return _Hash()(key) / BUCKET_NUM; }
    };

    using HashTable = std::unordered_map<K, V, SliceHash>;

   public:
    /**
     * @brief
     * @warning Rehashing causes the iterator to fail, but the change cannot be sensed. The iterator
     * should be updated in a timely manner during a large number of `insert()`, or `at()` should be
     * used.
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

    std::pair<iterator, bool> insert(K key, V val) {
        int index = hash(key);
        auto& shard = m_shards[index];
        auto& map = shard.m_map;

        std::unique_lock<__SharedMutex> guard(shard.m_lock);
        auto p = map.emplace(key, val);
        return {{index, p.first}, p.second};
    }

    iterator find(K key) {
        int index = hash(key);
        auto& shard = m_shards[index];
        auto& map = shard.m_map;

        std::shared_lock<__SharedMutex> guard(shard.m_lock);
        auto it = map.find(key);
        if (it != map.end()) {
            return {index, it};
        }
        return end();
    }

    V& at(K key) {
        int index = hash(key);
        auto& shard = m_shards[index];
        auto& map = shard.m_map;

        std::shared_lock<__SharedMutex> guard(shard.m_lock);
        return map.at(key);
    }

    V& operator[](K key) { return at(key); }

    /**
     * @brief Finds an element. If it does not exist, call `cotr_fn()` to insert a new element
     *
     * @tparam ConFn
     * @param key
     * @param cotr_fn
     * @return std::pair<iterator, bool>
     */
    template <typename ConFn>
    std::pair<iterator, bool> find_or_emplace(K key, ConFn&& ctor_fn) {
        auto iter = find(key);
        if (iter != end()) {
            return {iter, false};
        }

        int index = hash(key);
        auto& shard = m_shards[index];
        auto& map = shard.m_map;

        std::unique_lock<__SharedMutex> guard(shard.m_lock);
        auto it = map.find(key);
        if (it != map.end()) {
            return {{index, it}, false};
        }

        auto p = map.emplace(key, std::move(ctor_fn()));
        return {{index, p.first}, p.second};
    }

    void erase(K key) {
        auto it = find(key);
        erase(it);
    }

    void erase(iterator it) {
        if (it == end()) return;

        auto& shard = m_shards[it.hidx];
        auto& map = shard.m_map;

        std::unique_lock<__SharedMutex> guard(shard.m_lock);
        map.erase(it.it);
    }

    /**
     * @tparam F
     * @param f bool(std::pair<const K, V> &)，Returning false means the traversal is terminated.
     */
    template <typename F>
    void foreach_all(F&& f) {
        for (size_t i = 0; i < BucketNum; ++i) {
            auto& shard = m_shards[i];
            auto& map = shard.m_map;

            std::shared_lock<__SharedMutex> guard(shard.m_lock);
            for (auto& p : map) {
                if (!f(p)) {
                    return;
                }
            }
        }
    }

   private:
    struct CACHE_ALIGN Shard {
        __SharedMutex m_lock;
        HashTable m_map;
    };

    Shard m_shards[BucketNum];

    static size_t hash(K key) { return std::hash<K>()(key) % BucketNum; }
};

template <typename K, typename V, typename __SharedMutex = SharedMutex,
          typename _Hash = std::hash<K>>
class RandomAccessMap {
   public:
    class iterator {
       public:
        std::pair<const K, V>* operator->() {
            if (m->size() < index || m->values_[index].first != key) {
                update();
            }
            return &m->values_[index];
        }
        std::pair<const K, V>& operator*() {
            if (m->size() < index || m->values_[index].first != key) {
                update();
            }
            return m->values_[index];
        }
        bool operator==(const iterator& other) { return m == other.m && key == other.key; }
        bool operator!=(const iterator& other) { return m != other.m || key != other.key; }

       private:
        friend class RandomAccessMap;

        iterator(int index, K key, RandomAccessMap* m) : index(index), key(key), m(m) {}

        void update() {
            auto new_it = m->find(key);
            index = new_it.index;
            key = new_it.key;
            m = new_it.m;
        }

        int index;
        K key;
        RandomAccessMap* m;
    };

    bool empty() const { return values_.empty(); }
    size_t size() const { return values_.size(); }

    const iterator end() { return iterator(-1, K(), this); }

    std::pair<iterator, bool> emplace(const K& key, const V& value) {
        std::unique_lock<__SharedMutex> guard(lock_);
        auto it = key_to_index_.find(key);
        if (it != key_to_index_.end()) {
            return {iterator(it->second, key, this), false};
        }

        int index = key_to_index_[key] = values_.size() - 1;
        values_.push_back(value);
        return {iterator(index, key, this), true};
    }

    iterator find(const K& key) {
        std::shared_lock<__SharedMutex> guard(lock_);
        auto it = key_to_index_.find(key);
        if (it == key_to_index_.end()) {
            return end();
        }
        return iterator(it->second, key, this);
    }

    template <typename ConFn>
    std::pair<iterator, bool> find_or_emplace(const K& key, ConFn&& ctor_fn) {
        auto iter = find(key);
        if (iter != end()) {
            return {iter, false};
        }

        std::unique_lock<__SharedMutex> guard(lock_);
        auto it = key_to_index_.find(key);
        if (it != key_to_index_.end()) {
            return {iterator(it->second, key, this), false};
        }

        int index = values_.size();
        key_to_index_.emplace(key, index);

        values_.push_back({key, std::move(ctor_fn())});
        return {iterator(index, key, this), true};
    }

    V& at(const K& key) {
        std::shared_lock<__SharedMutex> guard(lock_);
        return key_to_index_.find(key)->second;
    }

    void erase(iterator it) {
        if (it == end()) return;

        std::unique_lock<__SharedMutex> guard(lock_);

        auto kit = key_to_index_.find(it.key);
        int index = kit->second;
        key_to_index_.erase(kit);
        values_[index] = std::move(values_.back());
        values_.pop_back();
        if (index < values_.size()) {
            key_to_index_[values_[index].first] = index;
        }
    }

    template <typename Genrator, typename F>
    std::vector<std::pair<K, V>> getRandomN(Genrator g, size_t n, F &&filter_fn) {
        DLOG_ASSERT(n <= values_.size());

        std::shared_lock<__SharedMutex> guard(lock_);

        std::vector<std::pair<K, V>> result;
        std::vector<bool> indices(values_.size(), false);
        std::uniform_int_distribution<> dis(0, values_.size() - 1);
        while (result.size() < n) {
            int index = dis(g);
            if (!indices[index]) {
                indices[index] = true;
                if (filter_fn(values_[index])) {
                    result.push_back(values_[index]);
                }
            }
        }
        return result;
    }

   private:
    __SharedMutex lock_;
    std::vector<std::pair<const K, V>> values_;
    std::unordered_map<K, int, _Hash> key_to_index_;
};