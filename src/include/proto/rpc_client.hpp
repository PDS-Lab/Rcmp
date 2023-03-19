#pragma once

#include "common.hpp"
#include "pool_impl.hpp"

namespace rpc_client {

void removePageCache(ClientContext& client_context, ClientToDaemonConnection& daemon_connection,
                     page_id_t page_id);

}