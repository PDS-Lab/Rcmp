#include "allocator.hpp"

#include <strings.h>

#include "log.hpp"

IDGenerator::IDGenerator() : m_gen_cur(0) {}

IDGenerator::IDGenerator(size_t size, size_t capacity, const void* data, size_t data_size)
    : m_gen_cur(0), m_size(size), m_capacity(capacity) {
    m_bset.assign(reinterpret_cast<const uint64_t*>(data),
                 reinterpret_cast<const uint64_t*>(data) + (data_size / sizeof(uint64_t)));
}

IDGenerator::id_t IDGenerator::gen() {
    if (m_size >= m_capacity) {
        return -1;
    }

    while (1) {
        int idx = ffsl(~m_bset[m_gen_cur]);
        if (idx == 0) {
            m_gen_cur = (m_gen_cur + 1) % m_bset.size();
        } else {
            ++m_size;
            return m_gen_cur * 64 + idx - 1;
        }
    }
}

void IDGenerator::recycle(IDGenerator::id_t id) {
    DLOG_ASSERT(m_bset[id / 64] & (1 << (id & 0x3F)), "IDGenerator double recycle");
    m_bset[id / 64] ^= (1 << (id & 0x3F));
    --m_size;
}

size_t IDGenerator::size() const { return m_size; }

size_t IDGenerator::capacity() const { return m_capacity; }

void IDGenerator::addCapacity(size_t n) {
    m_capacity += n;
    if (m_bset.capacity() * 64 < m_capacity) {
        m_bset.resize((m_capacity + 63) / 64, 0);
    }
}

void IDGenerator::reduceCapacity(size_t n) { m_capacity -= n; }

std::pair<const void*, size_t> IDGenerator::getRawData() const {
    return {m_bset.data(), m_bset.size() * sizeof(uint64_t)};
}

Allocator::Allocator(uintptr_t start_ptr, size_t total_size, size_t unit_size)
    : m_start_ptr(start_ptr), m_unit(unit_size) {
    addCapacity(total_size / unit_size);
}

Allocator::Allocator(uintptr_t start_ptr, size_t unit_size, size_t size, size_t capacity,
                     const void* data, size_t data_size)
    : m_start_ptr(start_ptr), m_unit(unit_size), IDGenerator(size, capacity, data, data_size) {}

uintptr_t Allocator::allocate(size_t n) {
    DLOG_ASSERT(n == 1, "Allocator can only allocate 1 element");
    IDGenerator::id_t id = gen();
    return m_start_ptr + id * m_unit;
}

void Allocator::deallocate(size_t n) { recycle(n / m_unit); }
