#pragma once

#include <utility>

#include "common.hpp"

class IDGenerator {
    using id_t = uint64_t;

public:
    /**
     * @brief 生成ID
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

    /**
     * @brief 加入更多的ID个数
     * 
     * @param n 
     */
    void LoadMoreID(size_t n);

    /**
     * @brief 除去未使用的ID个数
     * 
     * @param n 
     */
    void RemoveMoreID(size_t n);

    /**
     * @brief 导出内容，以便数据交换
     *
     * @return std::pair<const void*, size_t>
     */
    std::pair<const void*, size_t> const GetRawData();
};

/**
 * @brief 利用bitset实现的allocator
 *
 */
template <typename T>
class Allocator : public IDGenerator {};
