#include "allocator.hpp"

#include <strings.h>

#include <cstdint>

#include "log.hpp"
#include "utils.hpp"

IDGenerator::IDGenerator() : m_gen_cur(0), m_size(0) {}

IDGenerator::id_t IDGenerator::gen() {
    if (UNLIKELY(m_size + 1 > m_bset.size())) {
        return -1;
    }

    std::lock_guard<Mutex> guard(m_lck);

    size_t cur_tmp = m_gen_cur;
    do {
        size_t cur = m_gen_cur;
        m_gen_cur = (m_gen_cur + 1) % m_bset.size();
        auto ref = m_bset[cur];
        if (ref == false) {
            ref.flip();
            m_size += 1;
            return cur;
        }
    } while (cur_tmp != m_gen_cur);

    return -1;
}

IDGenerator::id_t IDGenerator::multiGen(size_t count) {
    if (count == 1) {
        return gen();
    }

    if (UNLIKELY(m_size + count > m_bset.size())) {
        return -1;
    }

    std::lock_guard<Mutex> guard(m_lck);

    id_t start = -1;
    size_t c = 0;
    size_t cur_tmp = m_gen_cur;

    do {
        size_t cur = m_gen_cur;
        m_gen_cur = (m_gen_cur + 1) % m_bset.size();
        if (m_bset[cur] == false) {
            if (c == 0) {
                start = cur;
            }
            ++c;
            if (c == count) {
                for (size_t k = start; k < start + count; ++k) {
                    m_bset[k].flip();
                }
                m_size += count;
                return start;
            }
        } else {
            c = 0;
        }

        // 防止回环
        if (m_gen_cur == 0) {
            c = 0;
        }

    } while (cur_tmp != m_gen_cur);

    return -1;
}

void IDGenerator::recycle(IDGenerator::id_t id) {
    DLOG_ASSERT(m_bset[id] == true, "IDGenerator double recycle");

    std::lock_guard<Mutex> guard(m_lck);

    m_bset[id].flip();
    m_size -= 1;
}

void IDGenerator::multiRecycle(id_t id, size_t count) {
    if (count == 1) {
        recycle(id);
        return;
    }

    std::lock_guard<Mutex> guard(m_lck);

    while (count--) {
        DLOG_ASSERT(m_bset[id] == true, "IDGenerator double recycle");
        m_bset[id].flip();
        id++;
    }
    m_size -= count;
}

bool IDGenerator::empty() const { return size() == 0; }

bool IDGenerator::full() const { return size() == capacity(); }

size_t IDGenerator::size() const { return m_size; }

size_t IDGenerator::capacity() const { return m_bset.size(); }

void IDGenerator::addCapacity(size_t n) {
    std::lock_guard<Mutex> guard(m_lck);
    m_bset.insert(m_bset.end(), n, false);
}

SingleAllocator::SingleAllocator(size_t total_size, size_t unit_size) : m_unit(unit_size) {
    addCapacity(total_size / unit_size);
}

uintptr_t SingleAllocator::allocate(size_t n) {
    DLOG_ASSERT(n == 1, "Must allocate 1 element");
    IDGenerator::id_t id = gen();
    if (UNLIKELY(id == -1)) {
        return -1;
    }
    return id * m_unit;
}

void SingleAllocator::deallocate(uintptr_t ptr) { recycle(ptr / m_unit); }
