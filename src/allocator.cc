#include "allocator.hpp"

#include <strings.h>

#include <cstdint>

#include "log.hpp"
#include "utils.hpp"

IDGenerator::IDGenerator() : m_gen_cur(0), m_size(0), m_capacity(0) {}

IDGenerator::IDGenerator(size_t size, size_t capacity, const void* data, size_t data_size)
    : m_gen_cur(0), m_size(size), m_capacity(capacity) {
    m_bset.assign(reinterpret_cast<const uint64_t*>(data),
                  reinterpret_cast<const uint64_t*>(data) + (data_size / sizeof(uint64_t)));
}

IDGenerator::id_t IDGenerator::gen() {
    if (UNLIKELY(m_size >= m_capacity)) {
        return -1;
    }

    std::lock_guard<Mutex> guard(m_lck);

    while (true) {
        int idx = ffsll(~m_bset[m_gen_cur]);
        if (idx == 0) {
            m_gen_cur = (m_gen_cur + 1) % m_bset.size();
        } else {
            ++m_size;
            m_bset[m_gen_cur] |= (1ull << (idx - 1));
            return m_gen_cur * 64 + idx - 1;
        }
    }
}

IDGenerator::id_t IDGenerator::multiGen(size_t count) {
    if (count == 1) {
        return gen();
    }

    if (UNLIKELY(m_size + count > m_capacity)) {
        return -1;
    }

    std::lock_guard<Mutex> guard(m_lck);

    id_t start = -1;
    size_t c = 0;
    size_t m_gen_cur_tmp = m_gen_cur;
    do {
        // TODO: ffsll优化
        for (int j = 0; j < 64; j++) {
            if ((m_bset[m_gen_cur] & (1ull << j)) == 0) {
                if (c == 0) {
                    start = m_gen_cur * 64 + j;
                }
                c++;
                if (c == count) {
                    // TODO: 赋值展开
                    for (size_t k = start; k < start + count; k++) {
                        m_bset[k / 64] |= (1ull << (k % 64));
                    }
                    m_size += count;
                    return start;
                }
            } else {
                c = 0;
            }
        }
        m_gen_cur = (m_gen_cur + 1) % m_bset.size();

        // 防止回环
        if (m_gen_cur == 0) {
            c = 0;
        }
    } while (m_gen_cur != m_gen_cur_tmp);

    return -1;
}

void IDGenerator::recycle(IDGenerator::id_t id) {
    DLOG_ASSERT(m_bset[id / 64] & (1ul << (id & 0x3F)), "IDGenerator double recycle");

    std::lock_guard<Mutex> guard(m_lck);

    m_bset[id / 64] ^= (1ul << (id % 64));
    --m_size;
}

void IDGenerator::multiRecycle(id_t id, size_t count) {
    if (count == 1) {
        recycle(id);
        return;
    }

    std::lock_guard<Mutex> guard(m_lck);

    while (count--) {
        DLOG_ASSERT(m_bset[id / 64] & (1ul << (id & 0x3F)), "IDGenerator double recycle");
        m_bset[id / 64] ^= (1ul << (id % 64));
        id++;
    }
    m_size -= count;
}

bool IDGenerator::empty() const { return size() == 0; }

bool IDGenerator::full() const { return size() == capacity(); }

size_t IDGenerator::size() const { return m_size; }

size_t IDGenerator::capacity() const { return m_capacity; }

void IDGenerator::addCapacity(size_t n) {
    std::lock_guard<Mutex> guard(m_lck);

    size_t old_capacity = capacity();
    if (old_capacity > 0) {
        m_bset[old_capacity / 64] &= (1ul << (old_capacity % 64)) - 1;
    }
    m_capacity += n;
    if (m_bset.size() * 64 < m_capacity) {
        m_bset.resize(div_ceil(m_capacity, 64), 0);
    }
    m_bset[m_capacity / 64] |= ~((1ul << (m_capacity % 64)) - 1);
}

void IDGenerator::reduceCapacity(size_t n) { m_capacity -= n; }

std::pair<const void*, size_t> IDGenerator::getRawData() const {
    return {m_bset.data(), m_bset.size() * sizeof(uint64_t)};
}

SingleAllocator::SingleAllocator(size_t total_size, size_t unit_size) : m_unit(unit_size) {
    addCapacity(total_size / unit_size);
}

SingleAllocator::SingleAllocator(size_t unit_size, size_t size, size_t capacity, const void* data,
                                 size_t data_size)
    : m_unit(unit_size), IDGenerator(size, capacity, data, data_size) {}

uintptr_t SingleAllocator::allocate(size_t n) {
    DLOG_ASSERT(n == 1, "Allocator can only allocate 1 element");
    IDGenerator::id_t id = gen();
    if (UNLIKELY(id == -1)) {
        return -1;
    }
    return id * m_unit;
}

void SingleAllocator::deallocate(uintptr_t ptr) { recycle(ptr / m_unit); }
