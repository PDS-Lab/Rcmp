#pragma once

#include <string>

namespace rchms {

class Options {
    std::string master_ip;  // MN的IP
    uint16_t master_port;   // MN的端口

    bool with_cxl = false;        // 是否注册为CXL客户端
    std::string cxl_devdax_path;  // CXL设备路径
};

}  // namespace rchms