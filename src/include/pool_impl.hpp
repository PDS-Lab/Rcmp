#pragma once

#include <unordered_set>

#include "common.hpp"
#include "concurrent_hashmap.hpp"
#include "log.hpp"
#include "msg_queue.hpp"

struct ClientToMasterConnection {
    std::string ip;
    uint16_t port;

    mac_id_t master_id;
};

struct ClientToDaemonConnection {
    MsgQueue send_msg_queue;  // 发给daemon的msg queue
    MsgQueue recv_msg_queue;  // 接收daemon消息的msg queue

    std::string ip;
    uint16_t port;

    rack_id_t rack_id;
    mac_id_t daemon_id;
};

struct ClientContext {
    rchms::ClientOptions options;

    std::string ip;
    uint16_t port;

    mac_id_t client_id;

    int cxl_devdax_fd;
    void *cxl_memory_addr;

    ClientToMasterConnection master_connection;
    ClientToDaemonConnection local_rack_daemon_connection;
    std::unordered_set<ClientToDaemonConnection *> other_rack_daemon_connection;
    ConcurrentHashMap<page_id_t, offset_t> page_table_cache;
};

struct rchms::PoolContext::__PoolContextImpl : public ClientContext {};