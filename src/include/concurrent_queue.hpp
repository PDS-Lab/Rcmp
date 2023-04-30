#pragma once

#include <atomic>
#include <cstddef>

#include "utils.hpp"

enum ConcurrentQueueProducerMode { SP, MP };
enum ConcurrentQueueConsumerMode { SC, MC };

template <typename T, size_t SZ, ConcurrentQueueProducerMode PROD_MODE,
          ConcurrentQueueConsumerMode CONS_MODE>
class ConcurrentQueue;

/**
 * @brief 单生产者单消费者队列
 *
 * @tparam T
 * @tparam SZ
 */
template <typename T, size_t SZ>
class ConcurrentQueue<T, SZ, ConcurrentQueueProducerMode::SP, ConcurrentQueueConsumerMode::SC> {
   public:
    ConcurrentQueue() : m_head(0), m_tail(0) {}
    ~ConcurrentQueue() = default;

    size_t capacity() const { return SZ; }

    bool tryEnqueue(T n) {
        uint32_t tail = m_tail.load(std::memory_order_relaxed);
        uint32_t next_tail = (tail + 1) % SZ;
        if (UNLIKELY(next_tail == m_head.load(std::memory_order_acquire))) {
            return false;  // 队列已满
        }
        m_data[tail] = std::move(n);
        m_tail.store(next_tail, std::memory_order_release);
        return true;
    }

    bool tryDequeue(T *n) {
        uint32_t head = m_head.load(std::memory_order_relaxed);
        if (UNLIKELY(head == m_tail.load(std::memory_order_acquire))) {
            return false;  // 队列已空
        }
        *n = std::move(m_data[head]);
        m_head.store((head + 1) % SZ, std::memory_order_release);
        return true;
    }

   private:
    std::atomic<uint32_t> m_head;
    T m_data[SZ];
    std::atomic<uint32_t> m_tail;
};

/**
 * @brief 多生产者单消费者队列
 *
 * @tparam T
 * @tparam SZ
 */
template <typename T, size_t SZ>
class ConcurrentQueue<T, SZ, ConcurrentQueueProducerMode::MP, ConcurrentQueueConsumerMode::SC> {
   public:
    ConcurrentQueue() {
        m_prod_head.raw = 0;
        m_prod_tail.raw = 0;
        m_cons_tail = 0;
    }
    ~ConcurrentQueue() = default;

    size_t capacity() const { return SZ; }

    void forceEnqueue(T n) {
        atomic_po_val_t h, oh, nh;

        oh = m_prod_head.fetch_add_both(1, 1, std::memory_order_acquire);
        while (UNLIKELY(oh.pos - m_cons_tail.load(std::memory_order_relaxed) >= SZ)) {
            h = m_prod_tail.load(std::memory_order_acquire);
            while (h.cnt == oh.cnt &&
                   !m_prod_tail.compare_exchange_weak(h, oh, std::memory_order_release,
                                                      std::memory_order_acquire)) {
            }
        }

        m_data[oh.pos % SZ] = std::move(n);

        oh = m_prod_tail.load(std::memory_order_acquire);
        do {
            h = m_prod_head.load(std::memory_order_relaxed);
            nh = oh;
            if ((++nh.cnt) == h.cnt) nh.pos = h.pos;
        } while (!m_prod_tail.compare_exchange_weak(oh, nh, std::memory_order_release,
                                                    std::memory_order_acquire));
    }

    bool tryEnqueue(T n) {
        atomic_po_val_t h, oh, nh;

        oh = m_prod_head.load(std::memory_order_acquire);
        do {
            if (UNLIKELY(oh.pos - m_cons_tail.load(std::memory_order_relaxed) >= SZ)) {
                return false;
            }
            nh.pos = oh.pos + 1;
            nh.cnt = oh.cnt + 1;
        } while (!m_prod_head.compare_exchange_weak(oh, nh, std::memory_order_acquire,
                                                    std::memory_order_acquire));

        m_data[oh.pos % SZ] = std::move(n);

        oh = m_prod_tail.load(std::memory_order_acquire);
        do {
            h = m_prod_head.load(std::memory_order_relaxed);
            nh = oh;
            if ((++nh.cnt) == h.cnt) nh.pos = h.pos;
        } while (!m_prod_tail.compare_exchange_weak(oh, nh, std::memory_order_release,
                                                    std::memory_order_acquire));

        return true;
    }

    bool tryDequeue(T *n) { return tryDequeue(n, n + 1) == 1; }

    template <typename Iter>
    uint32_t tryDequeue(Iter first, Iter last) {
        uint32_t l = 0;
        uint32_t count = std::distance(first, last);
        uint32_t ot = m_cons_tail.load(std::memory_order_acquire);
        l = std::min(count, m_prod_tail.load(std::memory_order_relaxed).pos - ot);
        if (l == 0) {
            return 0;
        }

        for (uint32_t i = 0; i < l; ++i) {
            *(first++) = std::move(m_data[(ot + i) % SZ]);
        }

        m_cons_tail.fetch_add(l, std::memory_order_release);
        return l;
    }

   private:
    atomic_po_val_t m_prod_head;
    atomic_po_val_t m_prod_tail;

    T m_data[SZ];

    std::atomic<uint32_t> m_cons_tail;
};

/**
 * @brief 多生产者多消费者队列
 *
 * @tparam T
 * @tparam SZ
 */
template <typename T, size_t SZ>
class ConcurrentQueue<T, SZ, ConcurrentQueueProducerMode::MP, ConcurrentQueueConsumerMode::MC> {
   public:
    ConcurrentQueue() {
        m_prod_head.raw = 0;
        m_prod_tail.raw = 0;
        m_cons_head.raw = 0;
        m_cons_tail.raw = 0;
    }
    ~ConcurrentQueue() = default;

    size_t capacity() const { return SZ; }

    void forceEnqueue(T n) {
        atomic_po_val_t h, oh, nh;

        oh = m_prod_head.fetch_add_both(1, 1, std::memory_order_acquire);
        while (UNLIKELY(oh.pos >= m_cons_tail.load(std::memory_order_relaxed).pos + SZ)) {
            h = m_prod_tail.load(std::memory_order_acquire);
            while (h.cnt == oh.cnt &&
                   !m_prod_tail.compare_exchange_weak(h, oh, std::memory_order_release,
                                                      std::memory_order_acquire)) {
            }
        }

        m_data[oh.pos % SZ] = std::move(n);

        oh = m_prod_tail.load(std::memory_order_acquire);
        do {
            h = m_prod_head.load(std::memory_order_relaxed);
            nh = oh;
            if ((++nh.cnt) == h.cnt) nh.pos = h.pos;
        } while (!m_prod_tail.compare_exchange_weak(oh, nh, std::memory_order_release,
                                                    std::memory_order_acquire));
    }

    bool tryEnqueue(T n) {
        atomic_po_val_t h, oh, nh;

        oh = m_prod_head.load(std::memory_order_acquire);
        do {
            if (UNLIKELY(oh.pos - m_cons_tail.load(std::memory_order_relaxed).pos >= SZ)) {
                return false;
            }
            nh.pos = oh.pos + 1;
            nh.cnt = oh.cnt + 1;
        } while (!m_prod_head.compare_exchange_weak(oh, nh, std::memory_order_acquire,
                                                    std::memory_order_acquire));

        m_data[oh.pos % SZ] = std::move(n);

        oh = m_prod_tail.load(std::memory_order_acquire);
        do {
            h = m_prod_head.load(std::memory_order_relaxed);
            nh = oh;
            if ((++nh.cnt) == h.cnt) nh.pos = h.pos;
        } while (!m_prod_tail.compare_exchange_weak(oh, nh, std::memory_order_release,
                                                    std::memory_order_acquire));

        return true;
    }

    bool tryDequeue(T *n) { return tryDequeue(n, n + 1) == 1; }

    template <typename Iter>
    uint32_t tryDequeue(Iter first, Iter last) {
        atomic_po_val_t t, ot, nt;
        uint32_t l = 0;
        uint32_t count = std::distance(first, last);

        ot = m_cons_head.load(std::memory_order_acquire);
        do {
            l = std::min(count, m_prod_tail.load(std::memory_order_relaxed).pos - ot.pos);
            if (l == 0) {
                return 0;
            }
            nt.pos = ot.pos + l;
            nt.cnt = ot.cnt + 1;
        } while (!m_cons_head.compare_exchange_weak(ot, nt, std::memory_order_acquire,
                                                    std::memory_order_acquire));

        for (uint32_t i = 0; i < l; ++i) {
            *(first++) = std::move(m_data[(ot.pos + i) % SZ]);
        }

        ot = m_cons_tail.load(std::memory_order_acquire);
        do {
            t = m_cons_head.load(std::memory_order_relaxed);
            nt = ot;
            if ((++nt.cnt) == t.cnt) nt.pos = t.pos;
        } while (!m_cons_tail.compare_exchange_weak(ot, nt, std::memory_order_release,
                                                    std::memory_order_acquire));

        return true;
    }

   private:
    atomic_po_val_t m_prod_head;
    atomic_po_val_t m_prod_tail;

    T m_data[SZ];

    atomic_po_val_t m_cons_head;
    atomic_po_val_t m_cons_tail;
};
