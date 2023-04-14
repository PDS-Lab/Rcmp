#include "proto/rpc_client.hpp"

namespace rpc_client {

GetCurrentWriteDataReply getCurrentWriteData(ClientContext& client_context,
                                             ClientToDaemonConnection& daemon_connection,
                                             GetCurrentWriteDataRequest& req) {
    auto reply_ptr = req.alloc_flex_resp(req.dio_write_size);

    memcpy(reply_ptr->data, req.dio_write_buf, req.dio_write_size);

    return *reply_ptr;
}

}  // namespace rpc_client
