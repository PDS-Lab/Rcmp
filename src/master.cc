#include "master_impl.hpp"

int main(int argc, char *argv[]) {
    rchms::MasterOptions options;
    
    MasterContext master_context;
    master_context.options = options;

    master_context.page_id_allocator.reset(new IDGenerator());
    master_context.cluster_manager.mac_id_allocator.reset(new IDGenerator());

    // TODO: 开始监听Daemon、client的连接

    return 0;
}