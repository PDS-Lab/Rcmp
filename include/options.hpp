#pragma once

#include <string>

// enable stat
#define RCMP_PERF_ON 1

namespace rcmp {

class ClientOptions {
   public:
    std::string client_ip;
    uint16_t client_port;

    uint32_t rack_id;

    // Whether to register as a CXL client (currently only `true` is supported)
    bool with_cxl = true;
    std::string cxl_devdax_path;
    size_t cxl_memory_size;
    int prealloc_fiber_num = 2;  // Number of pre-allocated boost coroutine
};

class DaemonOptions {
   public:
    std::string master_ip;
    uint16_t master_port = 31850;

    std::string daemon_ip;
    uint16_t daemon_port;

    uint32_t rack_id;

    // Whether to register as a CXL client (currently only `true` is supported)
    bool with_cxl = true;
    std::string cxl_devdax_path;
    size_t cxl_memory_size;

    // Maximum number of clients limit (limited by msgq communication area of shared memory)
    size_t max_client_limit = 32;
    size_t swap_zone_size = 64ul << 20;

    int prealloc_fiber_num = 32;     // Number of pre-allocated boost coroutine
    float heat_half_life_us = 1000;  // Page Heat decay coefficient
    float hot_swap_watermark = 3;    // Page Swap heat threshold

    int cm_qp_num = 2;  // Number of QPs connected to other daemons
};

class MasterOptions {
   public:
    std::string master_ip;
    uint16_t master_port = 31850;

    size_t max_cluster_mac_num = 1000;  // Maximum number of connected nodes in the cluster
    int prealloc_fiber_num = 16;        // Number of pre-allocated boost coroutine
};

}  // namespace rcmp