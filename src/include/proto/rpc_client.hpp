#pragma once

#include "common.hpp"
#include "impl.hpp"
#include "rpc_base.hpp"

namespace rpc_client {

void removePageCache(ClientContext& client_context, ClientToDaemonConnection& daemon_connection,
                     page_id_t page_id);

}