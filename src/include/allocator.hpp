#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include "common.hpp"
#include "log.hpp"

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
class Allocator : public IDGenerator {
   public:
    Allocator(size_t total_size, size_t unit_size);

    Allocator(size_t unit_size, size_t size, size_t capacity, const void* data, size_t data_size);

    uintptr_t allocate(size_t n);
    void deallocate(uintptr_t ptr);

   private:
    const size_t m_unit;
};
