#pragma once

#include "lock.hpp"
#include "log.hpp"
#include "utils.hpp"

template <typename T>
class ObjectPoolAllocator {
   public:
    using value_type = T;

    ObjectPoolAllocator() = default;

    template <typename U>
    ObjectPoolAllocator(const ObjectPoolAllocator<U>&) {}

    T* allocate(size_t n) {
        DLOG_ASSERT(n == 1, "Must allocate 1 element");

        if (pool.empty()) {
            return static_cast<T*>(::operator new(sizeof(T)));
        } else {
            T* obj = pool.back();
            pool.pop_back();
            return obj;
        }
    }

    void deallocate(T* p, size_t n) { pool.push_back(p); }

   private:
    class raw_ptr_vector : public std::vector<T*> {
       public:
        ~raw_ptr_vector() {
            for (auto* p : *this) {
                ::operator delete(p);
            }
        }
    };

    static thread_local raw_ptr_vector pool;
};

template <typename T>
thread_local typename ObjectPoolAllocator<T>::raw_ptr_vector ObjectPoolAllocator<T>::pool;

class IDGenerator {
   public:
    using id_t = uint64_t;

    IDGenerator() : m_gen_cur(0), m_size(0) {}

    bool empty() const { return size() == 0; }

    bool full() const { return size() == capacity(); }

    size_t size() const { return m_size; }

    size_t capacity() const { return m_bset.size(); }

    id_t Gen();

    id_t MultiGen(size_t count);

    void Recycle(id_t id);

    void MultiRecycle(id_t id, size_t count);

    void Expand(size_t n);

   private:
    size_t m_size;
    size_t m_gen_cur;
    std::vector<bool> m_bset;
    Mutex m_lck;
};

template <size_t UNIT_SZ>
class SingleAllocator : private IDGenerator {
   public:
    SingleAllocator(size_t total_size) { Expand(total_size / UNIT_SZ); }

    uintptr_t allocate(size_t n) {
        DLOG_ASSERT(n == 1, "Must allocate 1 element");
        IDGenerator::id_t id = Gen();
        if (UNLIKELY(id == -1)) {
            return -1;
        }
        return id * UNIT_SZ;
    }

    void deallocate(uintptr_t ptr, size_t n) { Recycle(ptr / UNIT_SZ); }
};

template <size_t SZ, size_t BucketNum = 4>
class RingArena {
   public:
    RingArena() {
        for (size_t i = 0; i < BucketNum; ++i) {
            m_bs[i].pv.raw = 0;
        }
    }

    const void* base() const { return m_bs; }

    void* allocate(size_t s) {
        DLOG_ASSERT(s <= block_size, "Can't allocate large than block size: %lu, %lu", s,
                    block_size);

        thread_local uint8_t b_cur = (reinterpret_cast<uintptr_t>(&b_cur) >> 5) % BucketNum;
        b_cur = (b_cur + 1) % BucketNum;
        uint8_t bc = b_cur;
        do {
            Block& b = m_bs[bc];
            atomic_po_val_t opv = b.pv.load(std::memory_order_acquire), npv;

            while (1) {
                npv = opv;
                npv.pos += s;
                if (npv.pos < block_size) {
                    ++npv.cnt;

                    if (b.pv.compare_exchange_weak(opv, npv, std::memory_order_acquire,
                                                   std::memory_order_acquire)) {
                        return b.b + opv.pos;
                    }

                } else {
                    bc = (bc + 1) % BucketNum;
                    break;
                }
            }

        } while (bc != b_cur);

        return nullptr;
    }

    void deallocate(void* p, size_t n) {
        uint8_t bc = div_floor(reinterpret_cast<uintptr_t>(p) - reinterpret_cast<uintptr_t>(m_bs),
                               sizeof(Block));
        DLOG_ASSERT(bc < BucketNum, "Out Of Memory");

        Block& b = m_bs[bc];
        atomic_po_val_t opv = b.pv.load(std::memory_order_acquire), npv;
        do {
            DLOG_ASSERT(opv.cnt != 0);
            npv = opv;
            if ((--npv.cnt) == 0) {
                npv.pos = 0;
            }
        } while (!b.pv.compare_exchange_weak(opv, npv, std::memory_order_release,
                                             std::memory_order_acquire));
    }

   private:
    static constexpr size_t block_size = SZ / BucketNum;

    struct Block {
        atomic_po_val_t pv;
        uint8_t b[block_size];
    };

    Block m_bs[BucketNum];
};

template <typename T>
class ObjectPool {
   public:
    T* pop() {
        if (pool.empty()) {
            return new T();
        } else {
            T* obj = pool.back();
            pool.pop_back();
            obj->clear();
            return obj;
        }
    }

    void put(T* p) { pool.push_back(p); }

   private:
    class raw_ptr_vector : public std::vector<T*> {
       public:
        ~raw_ptr_vector() {
            for (auto* p : *this) {
                delete (p);
            }
        }
    };

    static thread_local raw_ptr_vector pool;
};

template <typename T>
thread_local typename ObjectPool<T>::raw_ptr_vector ObjectPool<T>::pool;