#pragma once

#include "common.hpp"
#include "impl.hpp"

namespace rpc_client {

struct RemovePageCacheRequest {
    page_id_t page_id;
};
struct RemovePageCacheReply {};
/**
 * @brief 清理page cache
 *
 * @param client_context
 * @param daemon_connection
 * @param page_id
 */
RemovePageCacheReply removePageCache(ClientContext& client_context,
                                     ClientToDaemonConnection& daemon_connection,
                                     RemovePageCacheRequest& req);

struct GetCurrentWriteDataReply;
struct GetCurrentWriteDataRequest {
    void* dio_write_buf;
    size_t dio_write_size;
};
struct GetCurrentWriteDataReply {
    uint8_t data[0];
};
GetCurrentWriteDataReply getCurrentWriteData(ClientContext& client_context,
                                             ClientToDaemonConnection& daemon_connection,
                                             GetCurrentWriteDataRequest& req);

struct GetPagePastAccessFreqRequest {};
struct GetPagePastAccessFreqReply {
    page_id_t oldest_page_id;
    uint64_t last_access_ts;
};
GetPagePastAccessFreqReply getPagePastAccessFreq(ClientContext& client_context,
                                                 ClientToDaemonConnection& daemon_connection,
                                                 GetPagePastAccessFreqRequest& req);

}  // namespace rpc_client