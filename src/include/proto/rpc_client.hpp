#pragma once

#include "common.hpp"
#include "impl.hpp"
#include "rpc_base.hpp"

namespace rpc_client {

struct RemovePageCacheRequest : public RequestMsg {
    page_id_t page_id;
};
struct RemovePageCacheReply : public ResponseMsg {};
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
struct GetCurrentWriteDataRequest : public RequestMsg,
                                    detail::RawResponseReturn<GetCurrentWriteDataReply> {
    void* dio_write_buf;
    size_t dio_write_size;
};
struct GetCurrentWriteDataReply : public ResponseMsg {
    uint8_t data[0];
};
GetCurrentWriteDataReply getCurrentWriteData(ClientContext& client_context,
                                             ClientToDaemonConnection& daemon_connection,
                                             GetCurrentWriteDataRequest& req);

struct GetPagePastAccessFreqRequest : public RequestMsg {};
struct GetPagePastAccessFreqReply : public ResponseMsg {
    uint64_t last_access_ts;
};
GetPagePastAccessFreqReply getPagePastAccessFreq(ClientContext& client_context,
                                                 ClientToDaemonConnection& daemon_connection,
                                                 GetPagePastAccessFreqRequest& req);

}  // namespace rpc_client