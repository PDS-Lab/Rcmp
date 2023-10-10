#include "allocator.hpp"

#include "log.hpp"
#include "utils.hpp"

IDGenerator::id_t IDGenerator::Gen() {
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

IDGenerator::id_t IDGenerator::MultiGen(size_t count) {
    if (count == 1) {
        return Gen();
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

        // Preventing Loopback
        if (m_gen_cur == 0) {
            c = 0;
        }

    } while (cur_tmp != m_gen_cur);

    return -1;
}

void IDGenerator::Recycle(IDGenerator::id_t id) {
    DLOG_ASSERT(m_bset[id] == true, "IDGenerator double recycle");

    std::lock_guard<Mutex> guard(m_lck);

    m_bset[id].flip();
    m_size -= 1;
}

void IDGenerator::MultiRecycle(id_t id, size_t count) {
    if (count == 1) {
        Recycle(id);
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

void IDGenerator::Expand(size_t n) {
    std::lock_guard<Mutex> guard(m_lck);
    m_bset.insert(m_bset.end(), n, false);
}
