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
    int num_detect_pages;
    page_id_t pages[128];
};
struct GetPagePastAccessFreqReply {
    float avg_heat;
    page_id_t coldest_page_id;
    float coldest_page_heat;
    float coldest_page_rd_heat;
};
void getPagePastAccessFreq(ClientContext& client_context,
                           ClientToDaemonConnection& daemon_connection,
                           GetPagePastAccessFreqRequest& req,
                           ResponseHandle<GetPagePastAccessFreqReply>& resp_handle);

}  // namespace rpc_client