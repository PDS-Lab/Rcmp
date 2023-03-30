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

}  // namespace rpc_client