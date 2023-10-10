#pragma once

#include "utils.hpp"

namespace detail {

template <typename RpcFunc>
struct RpcCallerWrapperHelper {
    using FT = function_traits<RpcFunc>;
    using SelfContext = typename std::remove_reference<typename FT::template args_type<0>>::type;
    using PeerContext = typename std::remove_reference<typename FT::template args_type<1>>::type;
    using RequestType = typename std::remove_reference<typename FT::template args_type<2>>::type;
    using ResponseHandleType =
        typename std::remove_reference<typename FT::template args_type<3>>::type;

    using ResponseType = typename container_traits<ResponseHandleType>::type;
};

template <typename RpcFunc>
struct RpcCallerWrapper;

template <typename RpcFunc>
struct ErpcFuncWrapper : public RpcCallerWrapperHelper<RpcFunc> {
    static RpcFunc func;
    static bool registed;
};

template <typename RpcFunc>
bool ErpcFuncWrapper<RpcFunc>::registed = false;
template <typename RpcFunc>
RpcFunc ErpcFuncWrapper<RpcFunc>::func;

template <typename RpcFunc>
struct MsgqRpcFuncWrapper : public RpcCallerWrapperHelper<RpcFunc> {
    static RpcFunc func;
    static bool registed;
};

template <typename RpcFunc>
bool MsgqRpcFuncWrapper<RpcFunc>::registed = false;
template <typename RpcFunc>
RpcFunc MsgqRpcFuncWrapper<RpcFunc>::func;

/**
 * @brief Binding RPCs with the BIND_RPC_TYPE_STRUCT() macro
 *
 * @warning The call must be on a different line in the same file.
 */
#define BIND_RPC_TYPE_STRUCT(rpc_func)                                \
    template <>                                                       \
    struct detail::RpcCallerWrapper<decltype(rpc_func)>               \
        : public detail::RpcCallerWrapperHelper<decltype(rpc_func)> { \
        constexpr static uint8_t rpc_type = __LINE__;                 \
        static_assert(rpc_type != 0, "overflow");                     \
    };

/**
 * @brief Get the structure to which the rpc is bound.
 */
#define RPC_TYPE_STRUCT(rpc_func) ::detail::RpcCallerWrapper<decltype(rpc_func)>

}  // namespace detail
