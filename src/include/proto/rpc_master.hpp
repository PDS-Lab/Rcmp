#pragma once

#include "common.hpp"
#include "master_impl.hpp"
#include "rpc.hpp"

template <typename R, typename... Args>
struct function_traits_helper {
    static constexpr std::size_t count = sizeof...(Args);
    using result_type = R;
    using args_tuple_type = std::tuple<Args...>;
    template <std::size_t N>
    using args_type = typename std::tuple_element<N, std::tuple<Args...>>::type;
};

template <typename T>
struct function_traits;
template <typename R, typename... Args>
struct function_traits<R(Args...)> : public function_traits_helper<R, Args...> {};

namespace rpc_master {

struct JoinDaemonRequest : public RequestMsg {
    rack_id_t rack_id;
    bool with_cxl;
    size_t free_page_num;
};
struct JoinDaemonReply : public ResponseMsg {
    mac_id_t your_mac_id;
    mac_id_t my_mac_id;
};
/**
 * @brief 将daemon加入到集群中。在建立连接时调用。
 *
 * @param master_context
 * @param daemon_connection 需要解引用于从heap申请的对象，此后将由MasterContext维护其生命周期。
 * @param req
 * @return JoinDaemonReply
 */
JoinDaemonReply joinDaemon(MasterContext& master_context,
                           MasterToDaemonConnection& daemon_connection, JoinDaemonRequest& req);

struct JoinClientRequest : public RequestMsg {
    rack_id_t rack_id;
};
struct JoinClientReply : public ResponseMsg {
    mac_id_t mac_id;
};
/**
 * @brief 将client加入到集群中。在建立连接时调用。
 *
 * @param master_context
 * @param client_connection 需要解引用于从heap申请的对象，此后将由MasterContext维护其生命周期。
 * @param req
 * @return JoinClientReply
 */
JoinClientReply joinClient(MasterContext& master_context,
                           MasterToClientConnection& client_connection, JoinClientRequest& req);

struct AllocPageRequest : public RequestMsg {
    size_t slab_size;
};
struct AllocPageReply : public ResponseMsg {
    page_id_t page_id;
    bool need_self_alloc_page_memory;
};
/**
 * @brief
 * 申请一个page。该操作会希望在daemon端调用`allocPageMemory()`进行分配CXL物理地址。如果该daemon已满，本操作会随机向其他daemon发送该函数进行分配，此时原daemon不应将page
 * id加入到Page Table中。
 *
 * @param master_context
 * @param daemon_connection
 * @param req
 * @return AllocPageReply 如果page被派到daemon上，则`need_self_alloc_page_memory`为true
 */
AllocPageReply allocPage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                         AllocPageRequest& req);

struct FreePageRequest : public RequestMsg {
    page_id_t page_id;
};
struct FreePageReply : public ResponseMsg {};
/**
 * @brief 释放page。该操作需要保证daemon本身持有这个页时才能释放这个page。
 *
 * @param master_context
 * @param daemon_connection
 * @param page_id
 */
FreePageReply freePage(MasterContext& master_context, MasterToDaemonConnection& daemon_connection,
                       FreePageRequest& req);

struct GetRackDaemonByPageIDRequest : public RequestMsg {
    page_id_t page_id;
};
struct GetRackDaemonByPageIDReply : public ResponseMsg {
    char dest_daemon_ipv4[16];
    uint16_t dest_daemon_port;
    rack_id_t rack_id;
};
/**
 * @brief 根据page id获取对应rack的daemon的IPv4地址。该调用应在daemon的`远程直接访问`情况下使用。
 *
 * @param master_context
 * @param client_connection
 * @param page_id
 * @return GetRackDaemonByPageIDReply
 */
GetRackDaemonByPageIDReply getRackDaemonByPageID(MasterContext& master_context,
                                                 MasterToDaemonConnection& daemon_connection,
                                                 GetRackDaemonByPageIDRequest& req);

#define ERPC_REQUEST_HANDLER_WRAPPER_NAME(hdl) __erpc_request_handler_##hdl##_handler
#define ERPC_REQUEST_HANDLER_WRAPPER(handler)                                                      \
    void ERPC_REQUEST_HANDLER_WRAPPER_NAME(handler)(erpc::ReqHandle * req_handle, void* context) { \
        using ft = function_traits<decltype(handler)>;                                             \
        using result_type = ft::result_type;                                                       \
        using connection_type = ft::args_type<1>;                                                  \
        auto* rpc = reinterpret_cast<erpc::Rpc<erpc::CTransport>*>(context);                       \
        auto req_raw = req_handle->get_req_msgbuf();                                               \
        auto& resp_raw = req_handle->pre_resp_msgbuf_;                                             \
        auto req = reinterpret_cast<typename ft::args_type<2>>(req_raw->buf_);                     \
        auto resp = reinterpret_cast<result_type*>(resp_raw.buf_);                                 \
        MasterContext& master_context = MasterContext::getInstance();                              \
        MasterConnection* m_conn;                                                                  \
        bool ret =                                                                                 \
            master_context.cluster_manager.cluster_connect_table.find(req->mac_id, &m_conn);       \
        DLOG_ASSERT(ret, "Can't find connection %u", req->mac_id);                                 \
        connection_type& conn = *m_conn;                                                           \
        *resp = handler(master_context, conn, *req);                                               \
        rpc->resize_msg_buffer(&resp_raw, sizeof(result_type));                                    \
        rpc->enqueue_response(req_handle, &resp_raw);                                              \
    }

}  // namespace rpc_master