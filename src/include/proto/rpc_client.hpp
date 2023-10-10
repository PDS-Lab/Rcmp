#pragma once

#include "common.hpp"
#include "impl.hpp"
#include "proto/rpc_adaptor.hpp"

namespace rpc_client {

struct RemovePageCacheRequest {
    mac_id_t mac_id;
    page_id_t page_id;
};
struct RemovePageCacheReply {
    bool ret;
};
void removePageCache(ClientContext& client_context, ClientToDaemonConnection& daemon_connection,
                     RemovePageCacheRequest& req,
                     ResponseHandle<RemovePageCacheReply>& resp_handle);

struct GetCurrentWriteDataRequest {
    mac_id_t mac_id;
    const void* dio_write_buf;
    size_t dio_write_size;
};
struct GetCurrentWriteDataReply {
    uint8_t data[0];
};
void getCurrentWriteData(ClientContext& client_context, ClientToDaemonConnection& daemon_connection,
                         GetCurrentWriteDataRequest& req,
                         ResponseHandle<GetCurrentWriteDataReply>& resp_handle);

struct GetPagePastAccessFreqRequest {
    mac_id_t mac_id;
};
struct GetPagePastAccessFreqReply {
    page_id_t oldest_page_id;
    uint64_t last_access_ts;
};
void getPagePastAccessFreq(ClientContext& client_context,
                           ClientToDaemonConnection& daemon_connection,
                           GetPagePastAccessFreqRequest& req,
                           ResponseHandle<GetPagePastAccessFreqReply>& resp_handle);

}  // namespace rpc_client