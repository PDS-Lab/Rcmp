#pragma once

#include <cstdint>
#include <string>

#include "options.hpp"
#include "status.hpp"

namespace rchms {

using GAddr = uintptr_t;
constexpr static GAddr GNullPtr = 0;

/**
 * @brief 内存池客户端上下文
 */
class PoolContext;

/**
 * @brief 打开内存池。成功后返回内存池上下文指针，否则返回`nullptr`。返回对象由new操作生成，应由`Close()`关闭并删除
 *
 * @param master_ip MN的IP地址
 * @param master_port MN的端口号
 * @return PoolContext*
 */
PoolContext *Open(ClientOptions options);

/**
 * @brief 关闭内存池上下文
 *
 * @param pool_ctx
 */
void Close(PoolContext *pool_ctx);

class PoolContext {
   private:
    /**
     * @brief `PoolContext`内部实现
     * 
     */
    class PoolContextImpl;

   public:
    PoolContext(ClientOptions options);
    ~PoolContext();

    /**
     * @brief 申请内存。内存申请策略按照客户端所在机柜就近分配。申请失败返回`GNullPtr`
     *
     * @param size
     * @return GAddr
     */
    GAddr Alloc(size_t size);
    /**
     * @brief 读取`gaddr`地址、大小为`size`的数据到`buf`中
     *
     * @param gaddr
     * @param size
     * @param buf
     * @return Status
     */
    Status Read(GAddr gaddr, size_t size, void *buf);
    /**
     * @brief 从`buf`写入`gaddr`地址、大小为`size`的数据
     * 
     * @param gaddr 
     * @param size 
     * @param buf 
     * @return Status 
     */
    Status Write(GAddr gaddr, size_t size, void *buf);
    /**
     * @brief 释放内存
     * 
     * @param gaddr 
     * @param size 
     * @return Status 
     */
    Status Free(GAddr gaddr, size_t size);

    /**
     * @brief 测试数据发送
     * 
     * @param array 
     * @param size
     * @return Status 
     */
    Status __TestDataSend1(int *array, size_t size);

    /**
     * @brief 测试数据发送
     * 
     * @param array 
     * @param size
     * @return Status 
     */
    Status __TestDataSend2(int *array, size_t size);

   private:
    PoolContextImpl *__impl;
};

}  // namespace rchms