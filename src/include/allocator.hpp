#pragma once

#include <atomic>
#include <cstdint>
#include <utility>
#include <vector>

#include "common.hpp"
#include "lock.hpp"
#include "log.hpp"
#include "utils.hpp"

class IDGenerator {
   public:
    using id_t = uint64_t;

    IDGenerator();
    IDGenerator(size_t size, size_t capacity, const void* data, size_t data_size);

    /**
     * @brief 生成ID。生成失败返回-1。
     *
     * @return id_t
     */
    id_t gen();

    /**
     * @brief 回收ID
     *
     * @param id
     */
    void recycle(id_t id);

    bool empty() const;

    bool full() const;

    /**
     * @brief 返回分配的个数
     *
     * @return size_t
     */
    size_t size() const;

    /**
     * @brief 返回有效总容量
     *
     * @return size_t
     */
    size_t capacity() const;

    /**
     * @brief 加入更多的ID个数
     *
     * @param n
     */
    void addCapacity(size_t n);

    /**
     * @brief 除去未使用的ID个数
     *
     * @param n
     */
    void reduceCapacity(size_t n);

    /**
     * @brief 导出内容，以便数据交换。
     *
     * @warning 返回的指针会在类调用`loadMoreID`或者析构时释放。
     *
     * @return std::pair<const void*, size_t> {数据指针,数据字节大小}
     */
    std::pair<const void*, size_t> getRawData() const;

   private:
    size_t m_size;
    size_t m_capacity;
    size_t m_gen_cur;
    std::vector<uint64_t> m_bset;
};

/**
 * @brief 利用bitset实现的allocator
 *
 */
class SingleAllocator : public IDGenerator {
   public:
    SingleAllocator(size_t total_size, size_t unit_size);

    SingleAllocator(size_t unit_size, size_t size, size_t capacity, const void* data,
                    size_t data_size);

    uintptr_t allocate(size_t n);
    void deallocate(uintptr_t ptr);

   private:
    const size_t m_unit;
};

template <size_t SZ>
class RingAllocator {
   public:
    RingAllocator() = default;
    ~RingAllocator() = default;

    const void* base() const { return data; }

    void* alloc(size_t s) {
    retry:
        Header *inv_h, *h;
        atomic_po_val_t oh = m_prod_head.load(std::memory_order_acquire), nh, ph;
        do {
            inv_h = nullptr;
            nh.pos = oh.pos + s + sizeof(Header);
            nh.cnt = oh.cnt + 1;
            if (UNLIKELY(div_floor(oh.pos, SZ) != div_floor(nh.pos, SZ))) {
                // invalid tail
                size_t inv_s = align_ceil(oh.pos, SZ) - oh.pos;
                nh.pos += inv_s;
                inv_h = ath(oh.pos);
                h = ath(SZ);
            } else {
                h = ath(oh.pos);
            }
            if (UNLIKELY(nh.pos - m_tail > SZ)) {
                try_gc();
                if (UNLIKELY(nh.pos - m_tail > SZ))
                    return nullptr;
                else
                    goto retry;
            }
        } while (!m_prod_head.compare_exchange_weak(oh, nh, std::memory_order_acquire,
                                                    std::memory_order_acquire));

        h->invalid = false;
        h->release = false;
        h->s = s;
        if (inv_h != nullptr) {
            inv_h->invalid = true;
            inv_h->release = false;
        }

        oh = m_prod_tail.load(std::memory_order_acquire);
        do {
            ph = m_prod_head.load(std::memory_order_relaxed);
            nh = oh;
            if ((++nh.cnt) == ph.cnt) {
                nh.pos = ph.pos;
            }
        } while (!m_prod_tail.compare_exchange_weak(oh, nh, std::memory_order_release,
                                                    std::memory_order_acquire));

        if (UNLIKELY(inv_h != nullptr)) {
            free(inv_h->data);
        }

        return h->data;
    }

    void free(void* p) {
        Header* h = (Header*)((char*)p - offsetof(Header, data));
        h->release = true;
    }

   private:
    struct Header {
        bool invalid : 1;
        bool release : 1;
        size_t s : 62;
        char data[0];
    };

    atomic_po_val_t m_prod_head;
    atomic_po_val_t m_prod_tail;

    char data[SZ];

    Mutex m_gc_lck;
    size_t m_tail;

    void try_gc() {
        if (!m_gc_lck.try_lock()) {
            return;
        }

        auto tail = m_tail;
        Header* h = ath(tail);
        while (tail < m_prod_tail.load().pos && h->release) {
            if (UNLIKELY(h->invalid)) {
                tail = align_ceil(tail, SZ);
            } else {
                tail += h->s + sizeof(Header);
            }
            if (tail - m_tail > SZ / 4) m_tail = tail;
            h = ath(tail);
        }
        m_tail = tail;

        m_gc_lck.unlock();
    }

    Header* ath(size_t i) const { return (Header*)(data + (i % SZ)); }
};