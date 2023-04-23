#include <cstdint>

#include "rpc_client.hpp"
#include "rpc_daemon.hpp"
#include "rpc_master.hpp"

namespace detail {

template <typename RpcFunc>
struct RpcCallerWrapperHelper {
    using FT = function_traits<RpcFunc>;
    using RequestType = typename std::remove_reference<typename FT::template args_type<2>>::type;
    using ResponseType = typename std::remove_reference<typename FT::result_type>::type;
};

template <typename RpcFunc>
struct RpcCallerWrapper;

/**
 * @brief 使用BIND_RPC_TYPE_STRUCT()宏绑定RPC
 *
 * @warning 调用时必须位于同一文件内的不同行
 */
#define BIND_RPC_TYPE_STRUCT(rpc_func)                                \
    template <>                                                       \
    struct detail::RpcCallerWrapper<decltype(rpc_func)>               \
        : public detail::RpcCallerWrapperHelper<decltype(rpc_func)> { \
        constexpr static uint8_t rpc_type = __LINE__;                 \
        static_assert(rpc_type != 0, "overflow");                     \
    };

/**
 * @brief 获取rpc绑定的的结构体
 *
 */
#define RPC_TYPE_STRUCT(rpc_func) ::detail::RpcCallerWrapper<decltype(rpc_func)>

}  // namespace detail

/******************* 绑定RPC函数 **********************/

BIND_RPC_TYPE_STRUCT(rpc_master::joinDaemon);
BIND_RPC_TYPE_STRUCT(rpc_master::joinClient);
BIND_RPC_TYPE_STRUCT(rpc_master::allocPage);
BIND_RPC_TYPE_STRUCT(rpc_master::freePage);
// BIND_RPC_TYPE_STRUCT(rpc_master::getRackDaemonByPageID);
BIND_RPC_TYPE_STRUCT(rpc_master::latchRemotePage);
BIND_RPC_TYPE_STRUCT(rpc_master::unLatchRemotePage);

BIND_RPC_TYPE_STRUCT(rpc_daemon::joinRack);
BIND_RPC_TYPE_STRUCT(rpc_daemon::crossRackConnect);
BIND_RPC_TYPE_STRUCT(rpc_daemon::getPageCXLRefOrProxy);
BIND_RPC_TYPE_STRUCT(rpc_daemon::allocPage);
BIND_RPC_TYPE_STRUCT(rpc_daemon::freePage);
BIND_RPC_TYPE_STRUCT(rpc_daemon::allocPageMemory);
BIND_RPC_TYPE_STRUCT(rpc_daemon::alloc);
BIND_RPC_TYPE_STRUCT(rpc_daemon::free);
BIND_RPC_TYPE_STRUCT(rpc_daemon::getPageRDMARef);
BIND_RPC_TYPE_STRUCT(rpc_daemon::__testdataSend1);
BIND_RPC_TYPE_STRUCT(rpc_daemon::__testdataSend2);
BIND_RPC_TYPE_STRUCT(rpc_daemon::__notifyPerf);
BIND_RPC_TYPE_STRUCT(rpc_daemon::__stopPerf);

BIND_RPC_TYPE_STRUCT(rpc_client::removePageCache);
BIND_RPC_TYPE_STRUCT(rpc_client::getCurrentWriteData);
BIND_RPC_TYPE_STRUCT(rpc_client::getPagePastAccessFreq);
