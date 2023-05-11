#pragma once

#include <string>

namespace rchms {

class ClientOptions {
   public:
    std::string client_ip;
    uint16_t client_port;  // msgq port

    uint32_t rack_id;

    bool with_cxl = false;        // 是否注册为CXL客户端
    std::string cxl_devdax_path;  // CXL设备路径
    size_t cxl_memory_size;       // CXL设备大小
};

class DaemonOptions {
   public:
    std::string master_ip;  // MN的IP
    uint16_t master_port;   // MN的端口

    std::string daemon_ip;
    std::string daemon_rdma_ip;  // daemon的rdma网卡ip
    uint16_t daemon_port;        // erpc port

    uint32_t rack_id;

    bool with_cxl = false;        // 是否注册为CXL客户端
    std::string cxl_devdax_path;  // CXL设备路径
    size_t cxl_memory_size;       // CXL设备大小

    size_t max_client_limit;  // 最大client数量限制
    size_t swap_zone_size;    // 交换区大小

    int prealloc_cort_num;      // 预分配协程个数
    float hot_decay_lambda;     // 热度衰减系数
    size_t hot_swap_watermark;  // 热度阈值
};

class MasterOptions {
   public:
    std::string master_ip;       // MN的IP
    std::string master_rdma_ip;  // rdma网卡IP
    uint16_t master_port;        // MN的端口

    size_t max_cluster_mac_num;  // 集群中最多连接个数
};

}  // namespace rchms