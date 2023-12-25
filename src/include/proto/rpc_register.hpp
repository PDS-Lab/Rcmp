#pragma once

#include "rpc_caller.hpp"
#include "rpc_client.hpp"
#include "rpc_daemon.hpp"
#include "rpc_master.hpp"

/******************* Binding RPC Functions **********************/

BIND_RPC_TYPE_STRUCT(rpc_master::joinDaemon);
BIND_RPC_TYPE_STRUCT(rpc_master::joinClient);
BIND_RPC_TYPE_STRUCT(rpc_master::allocPage);
BIND_RPC_TYPE_STRUCT(rpc_master::freePage);
BIND_RPC_TYPE_STRUCT(rpc_master::latchRemotePage);
BIND_RPC_TYPE_STRUCT(rpc_master::unLatchRemotePage);
BIND_RPC_TYPE_STRUCT(rpc_master::tryMigratePage);
BIND_RPC_TYPE_STRUCT(rpc_master::MigratePageDone);

BIND_RPC_TYPE_STRUCT(rpc_daemon::joinRack);
BIND_RPC_TYPE_STRUCT(rpc_daemon::crossRackConnect);
BIND_RPC_TYPE_STRUCT(rpc_daemon::getPageCXLRefOrProxy);
BIND_RPC_TYPE_STRUCT(rpc_daemon::allocPage);
BIND_RPC_TYPE_STRUCT(rpc_daemon::freePage);
BIND_RPC_TYPE_STRUCT(rpc_daemon::allocPageMemory);
BIND_RPC_TYPE_STRUCT(rpc_daemon::alloc);
BIND_RPC_TYPE_STRUCT(rpc_daemon::free);
BIND_RPC_TYPE_STRUCT(rpc_daemon::getPageRDMARef);
BIND_RPC_TYPE_STRUCT(rpc_daemon::delPageRDMARef);
BIND_RPC_TYPE_STRUCT(rpc_daemon::tryDelPage);
BIND_RPC_TYPE_STRUCT(rpc_daemon::migratePage);
BIND_RPC_TYPE_STRUCT(rpc_daemon::__testdataSend1);
BIND_RPC_TYPE_STRUCT(rpc_daemon::__testdataSend2);
BIND_RPC_TYPE_STRUCT(rpc_daemon::__notifyPerf);
BIND_RPC_TYPE_STRUCT(rpc_daemon::__stopPerf);

BIND_RPC_TYPE_STRUCT(rpc_client::removePageCache);
BIND_RPC_TYPE_STRUCT(rpc_client::getCurrentWriteData);
BIND_RPC_TYPE_STRUCT(rpc_client::getPagePastAccessFreq);