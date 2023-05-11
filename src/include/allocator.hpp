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

    /**
     * @brief 生成ID。生成失败返回-1。
     *
     * @return id_t
     */
    id_t gen();

    /**
     * @brief 生成连续ID。生成失败返回-1。
     *
     * @return id_t
     */
    id_t multiGen(size_t count);

    /**
     * @brief 回收ID
     *
     * @param id
     */
    void recycle(id_t id);

    void multiRecycle(id_t id, size_t count);

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

   private:
    size_t m_size;
    size_t m_gen_cur;
    std::vector<bool> m_bset;
    Mutex m_lck;
};

/**
 * @brief 利用bitset实现的allocator
 *
 */
class SingleAllocator : public IDGenerator {
   public:
    SingleAllocator(size_t total_size, size_t unit_size);

    uintptr_t allocate(size_t n);
    void deallocate(uintptr_t ptr);

   private:
    const size_t m_unit;
};

template <size_t SZ>
class RingAllocator {
   public:
    RingAllocator() {
        m_prod_head.raw = 0;
        m_prod_tail.raw = 0;
        m_tail = 0;
    }
    ~RingAllocator() = default;

    const void* base() const { return data; }

    void* allocate(size_t s) {
        bool f = false;
    retry:
        Header *inv_h, *h;
        atomic_po_val_t oh = m_prod_head.load(std::memory_order_acquire), nh, ph;
        do {
            inv_h = nullptr;
            nh.pos = oh.pos + s + sizeof(Header);
            nh.cnt = oh.cnt + 1;
            if (UNLIKELY(nh.pos % SZ != 0 && div_floor(oh.pos, SZ) != div_floor(nh.pos, SZ))) {
                // invalid tail
                size_t inv_s = align_ceil(oh.pos, SZ) - oh.pos;
                nh.pos += inv_s;
                inv_h = at(oh.pos);
                h = at(SZ);
            } else {
                h = at(oh.pos);
            }
            uint64_t d = nh.pos - m_tail;
            if (UNLIKELY(!f && d > SZ / 2)) {
                f = true;
                if (try_gc()) {
                    goto retry;
                }
            }
            if (UNLIKELY(d > SZ)) {
                if (try_gc())
                    goto retry;
                else
                    return nullptr;
            }
        } while (!m_prod_head.compare_exchange_weak(oh, nh, std::memory_order_acquire,
                                                    std::memory_order_acquire));

        h->invalid = false;
        h->release = false;
        h->s = s;

        if (UNLIKELY(inv_h != nullptr)) {
            inv_h->invalid = true;
            inv_h->release = false;
        }

        m_prod_tail.fetch_add_cnt(1, std::memory_order_release);

        if (UNLIKELY(inv_h != nullptr)) {
            deallocate(inv_h->data);
        }

        return h->data;
    }

    void deallocate(void* p) {
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

    char data[SZ + sizeof(Header)];

    Mutex m_gc_lck;
    size_t m_tail;

    bool try_gc() {
        if (!m_gc_lck.try_lock()) {
            return false;
        }

        atomic_po_val_t oh = m_prod_tail.load(std::memory_order_acquire),
                        ph = m_prod_head.load(std::memory_order_relaxed);
        while (oh.cnt == ph.cnt &&
               !m_prod_tail.compare_exchange_weak(oh, ph, std::memory_order_release,
                                                  std::memory_order_acquire)) {
        }

        auto tail = m_tail;
        Header* h = at(tail);
        while (tail < m_prod_tail.load().pos && h->release) {
            if (UNLIKELY(h->invalid)) {
                tail = align_ceil(tail, SZ);
            } else {
                tail += h->s + sizeof(Header);
            }
            // if (tail - m_tail > SZ / 4) m_tail = tail;
            h = at(tail);
        }
        m_tail = tail;

        m_gc_lck.unlock();
        return true;
    }

    Header* at(size_t i) const { return (Header*)(data + (i % SZ)); }
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
        // thread local
        uint8_t b_cur = reinterpret_cast<uintptr_t>(&b_cur) % BucketNum, bc = b_cur;
        do {
            Block& b = m_bs[bc];
            atomic_po_val_t opv = b.pv.load(std::memory_order_acquire), npv;

            while (1) {
                npv = opv;
                npv.pos += s;
                if (npv.pos < block_size) {
                    npv.cnt++;

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

    void deallocate(void* p) {
        uint8_t bc = div_floor(reinterpret_cast<uintptr_t>(p) - reinterpret_cast<uintptr_t>(m_bs),
                               sizeof(Block));
        DLOG_ASSERT(bc < BucketNum, "Out Of Memory");

        Block& b = m_bs[bc];
        atomic_po_val_t opv = b.pv.load(std::memory_order_acquire), npv;
        do {
            npv = opv;
            if ((--npv.cnt) == 0) {
                npv.pos = 0;
            }
        } while (!b.pv.compare_exchange_weak(opv, npv, std::memory_order_release,
                                             std::memory_order_acquire));
    }

   private:
    static const size_t block_size = SZ / BucketNum;

    struct Block {
        atomic_po_val_t pv;
        uint8_t b[block_size];
    };

    Block m_bs[BucketNum];
};