#pragma once

#include <string>

namespace rchms {

class ClientOptions {
   public:
    std::string master_ip;  // MN的IP
    uint16_t master_port;   // MN的端口

    bool with_cxl = false;        // 是否注册为CXL客户端
    std::string cxl_devdax_path;  // CXL设备路径
};

class DaemonOptions {
   public:
    std::string master_ip;  // MN的IP
    uint16_t master_port;   // MN的端口

    bool with_cxl = false;        // 是否注册为CXL客户端
    std::string cxl_devdax_path;  // CXL设备路径
    size_t cxl_memory_size;       // CXL设备大小
    size_t cxl_msg_queue_size;    // CXL内存上msg queue的大小

    size_t max_client_limit;  // 最大client数量限制
    size_t swap_zone_size;    // 交换区大小
};

}  // namespace rchms