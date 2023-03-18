#pragma once

#include <cstddef>

enum ConcurrentQueueProducerMode { SP, MP };
enum ConcurrentQueueConsumerMode { SC, MC };

template <typename T, size_t SZ, ConcurrentQueueProducerMode PROD_MODE,
          ConcurrentQueueConsumerMode CONS_MODE>
class ConcurrentQueue {
   public:
    bool empty() const;
    size_t size() const;
    bool try_enqueue(T el);
    bool try_dequeue(T *el);
};