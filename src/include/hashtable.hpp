#include <iostream>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "lock.hpp"
#include "log.hpp"

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

        int index = key_to_index_[key] = values_.size() - 1;
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

    template <typename Genrator>
    std::vector<std::pair<K, V>> getRandomN(Genrator g, size_t n) {
        DLOG_ASSERT(n <= values_.size());

        std::shared_lock<__SharedMutex> guard(lock_);

        std::vector<std::pair<K, V>> result;
        std::vector<bool> indices(values_.size(), false);
        std::uniform_int_distribution<> dis(0, values_.size() - 1);
        while (result.size() < n) {
            int index = dis(g);
            if (!indices[index]) {
                indices[index] = true;
                result.push_back(values_[index]);
            }
        }
        return result;
    }

   private:
    __SharedMutex lock_;
    std::vector<std::pair<const K, V>> values_;
    std::unordered_map<K, int, _Hash> key_to_index_;
};