#pragma once

#include <string>

namespace rchms {

class ClientOptions {
   public:
    std::string client_ip;
    uint16_t client_port;

    uint32_t rack_id;

    bool with_cxl = true;         // 是否注册为CXL客户端
    std::string cxl_devdax_path;  // CXL设备路径
    size_t cxl_memory_size;       // CXL设备大小
    int prealloc_fiber_num = 2;   // 预分配协程个数
};

class DaemonOptions {
   public:
    std::string master_ip;         // MN的IP
    uint16_t master_port = 31850;  // MN的端口

    std::string daemon_ip;
    uint16_t daemon_port;

    uint32_t rack_id;

    bool with_cxl = true;         // 是否注册为CXL客户端
    std::string cxl_devdax_path;  // CXL设备路径
    size_t cxl_memory_size;       // CXL设备大小

    size_t max_client_limit = 32;        // 最大client数量限制
    size_t swap_zone_size = 64ul << 20;  // 交换区大小

    int prealloc_fiber_num = 16;     // 预分配协程个数
    float hot_decay_lambda = 0.04;  // 热度衰减系数
    size_t hot_swap_watermark = 3;  // 热度阈值
};

class MasterOptions {
   public:
    std::string master_ip;         // MN的IP
    uint16_t master_port = 31850;  // MN的端口

    size_t max_cluster_mac_num = 1000;  // 集群中最多连接个数
    int prealloc_fiber_num = 16;         // 预分配协程个数
};

}  // namespace rchms