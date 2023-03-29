#pragma once

#include <memory>

#include "allocator.hpp"
#include "common.hpp"
#include "concurrent_hashmap.hpp"
#include "eRPC/erpc.h"
#include "log.hpp"
#include "options.hpp"

struct PageRackMetadata {
    uint32_t rack_id;
};

struct MasterConnection {
    std::string ip;
    uint16_t port;

    virtual ~MasterConnection() = default;
};

struct MasterToClientConnection : public MasterConnection {
    rack_id_t rack_id;
    mac_id_t client_id;
};

struct MasterToDaemonConnection : public MasterConnection {
    rack_id_t rack_id;
    mac_id_t daemon_id;
};

struct RackMacTable {
    bool with_cxl;
    MasterToDaemonConnection *daemon_connect;
    size_t max_free_page_num;
    size_t current_allocated_page_num;
    std::vector<MasterToClientConnection *> client_connect_table;
};

struct ClusterManager {
    ConcurrentHashMap<rack_id_t, RackMacTable *> cluster_rack_table;
    ConcurrentHashMap<mac_id_t, MasterConnection *> cluster_connect_table;
    std::unique_ptr<IDGenerator> mac_id_allocator;
};

struct MasterContext {
    rchms::MasterOptions options;

    mac_id_t master_id;  // 节点id，由master分配

    ClusterManager cluster_manager;

    std::unique_ptr<IDGenerator> page_id_allocator;
    ConcurrentHashMap<page_id_t, PageRackMetadata *> page_directory;

    struct {
        volatile bool running;
        std::unique_ptr<erpc::NexusWrap> nexus;
        std::vector<erpc::IBRpcWrap> rpc_set;
    } erpc_ctx;

    MasterContext() = default;
    MasterContext(MasterContext &) = delete;
    MasterContext(MasterContext &&) = delete;
    MasterContext &operator=(MasterContext &) = delete;
    MasterContext &operator=(MasterContext &&) = delete;

    static MasterContext &getInstance();
};