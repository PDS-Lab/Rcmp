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

    bool tryEnqueue(T n) {
        int tail = m_tail.load(std::memory_order_relaxed);
        int next_tail = (tail + 1) % SZ;
        if (UNLIKELY(next_tail == m_head.load(std::memory_order_acquire))) {
            return false;  // 队列已满
        }
        m_data[tail] = n;
        m_tail.store(next_tail, std::memory_order_release);
        return true;
    }

    bool tryDequeue(T *n) {
        int head = m_head.load(std::memory_order_relaxed);
        if (UNLIKELY(head == m_tail.load(std::memory_order_acquire))) {
            return false;  // 队列已空
        }
        *n = m_data[head];
        m_head.store((head + 1) % SZ, std::memory_order_release);
        return true;
    }

   private:
    T m_data[SZ];
    CACHE_ALIGN std::atomic<int> m_head;
    CACHE_ALIGN std::atomic<int> m_tail;
};