#pragma once

#include <list>
#include <memory>
#include <unordered_set>

#include "allocator.hpp"
#include "common.hpp"
#include "concurrent_hashmap.hpp"
#include "config.hpp"
#include "eRPC/erpc.h"
#include "log.hpp"
#include "msg_queue.hpp"
#include "options.hpp"

struct DaemonConnection {
    std::string ip;
    uint16_t port;

    virtual ~DaemonConnection() = default;
};

struct DaemonToMasterConnection : public DaemonConnection {
    mac_id_t master_id;
};

struct DaemonToClientConnection : public DaemonConnection {
    MsgQueue send_msg_queue;  // 发给client的msg queue
    MsgQueue recv_msg_queue;  // 接收client消息的msg queue

    mac_id_t client_id;
};

struct DaemonToDaemonConnection : public DaemonConnection {
    rack_id_t rack_id;
    mac_id_t daemon_id;
};

struct PageMetadata {
    offset_t cxl_memory_offset;
    Allocator slab_allocator;
    std::unordered_set<DaemonToClientConnection *> ref_client;

    PageMetadata(size_t slab_size);
};

struct DaemonContext {
    rchms::DaemonOptions options;

    mac_id_t daemon_id;  // 节点id，由master分配

    int cxl_devdax_fd;
    void *cxl_memory_addr;

    size_t total_page_num;     // 所有page的个数
    size_t max_swap_page_num;  // swap区的page个数
    size_t max_data_page_num;  // 所有可用数据页个数

    size_t current_used_page_num;       // 当前使用的数据页个数
    size_t current_used_swap_page_num;  // 当前正在swap的页个数

    DaemonToMasterConnection master_connection;
    std::unique_ptr<Allocator> cxl_msg_queue_allocator;
    std::vector<DaemonToClientConnection *> client_connect_table;
    std::vector<DaemonToDaemonConnection *> other_daemon_connect_table;
    std::unique_ptr<Allocator> cxl_page_allocator;
    ConcurrentHashMap<page_id_t, PageMetadata *> page_table;

    std::array<std::list<page_id_t>, page_size / min_slab_size> can_alloc_slab_class_lists;

    struct {
        volatile bool running;
        int master_session;
        std::unique_ptr<erpc::NexusWrap> nexus;
        std::vector<erpc::IBRpcWrap> rpc_set;
    } erpc_ctx;

    DaemonContext() = default;
    DaemonContext(DaemonContext &) = delete;
    DaemonContext(DaemonContext &&) = delete;
    DaemonContext &operator=(DaemonContext &) = delete;
    DaemonContext &operator=(DaemonContext &&) = delete;

    static DaemonContext &getInstance();
};