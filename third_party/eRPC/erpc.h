#pragma once

#include <memory>
#include <string>

namespace erpc {
class Nexus;
class ReqHandle;
class IBRpc;
class MsgBuffer;

using erpc_req_func_t = void (*)(ReqHandle *req_handle, void *context);
using erpc_cont_func_t = void (*)(void *context, void *tag);

enum erpc_req_func_type_t : uint8_t { kForeground, kBackground };

class SMHandlerWrap {
    friend class IBRpcWrap;

   public:
    void set_null();
    void set_empty();

   private:
    void *raw_sm_handler_;
};

class NexusWrap {
    friend class IBRpcWrap;

   public:
    NexusWrap(std::string uri);
    NexusWrap(NexusWrap &&nexus);
    ~NexusWrap();

    void register_req_func(uint8_t type, erpc_req_func_t req_func,
                           erpc_req_func_type_t req_func_type = erpc_req_func_type_t::kForeground);

   private:
    std::unique_ptr<Nexus> nexus_;
};

class MsgBufferWrap {
    friend class IBRpcWrap;
    friend class ReqHandleWrap;

   public:
    MsgBufferWrap(MsgBuffer *buffer);
    ~MsgBufferWrap();

    void *get_buf() const;
    void set(MsgBufferWrap msgbuf);

   private:
    MsgBuffer *buffer_;
};

class ReqHandleWrap {
    friend class IBRpcWrap;

   public:
    ReqHandleWrap(ReqHandle *req_handle);
    ~ReqHandleWrap();

    uint8_t get_server_rpc_id() const;
    int get_server_session_num() const;

    const MsgBufferWrap get_req_msgbuf() const;
    MsgBufferWrap get_pre_resp_msgbuf() const;
    MsgBufferWrap get_dyn_resp_msgbuf() const;

   private:
    ReqHandle *req_hdl_;
};

class IBRpcWrap {
   public:
    static size_t kMaxDataPerPkt;

    IBRpcWrap(NexusWrap *nexus, void *context, uint8_t rpc_id, SMHandlerWrap sm_handler,
              uint8_t phy_port = 0);
    IBRpcWrap(IBRpcWrap &&rpc);
    ~IBRpcWrap();

    int create_session(std::string remote_uri, uint8_t rem_rpc_id);
    void run_event_loop_once();
    MsgBufferWrap alloc_msg_buffer_or_die(size_t max_data_size);
    void enqueue_request(int session_num, uint8_t req_type, MsgBufferWrap req_msgbuf,
                         MsgBufferWrap resp_msgbuf, erpc_cont_func_t cont_func, void *tag,
                         size_t cont_etid = 8);
    void enqueue_response(ReqHandleWrap req_handle, MsgBufferWrap resp_msgbuf);
    void resize_msg_buffer(MsgBufferWrap msg_buffer, size_t new_data_size);
    void free_msg_buffer(MsgBufferWrap msg_buffer);

   private:
    std::unique_ptr<IBRpc> rpc_;
};

inline std::string concat_server_uri(std::string ip, uint16_t port) {
    return ip + ':' + std::to_string(port);
}

}  // namespace erpc