#include "impl.hpp"

ClientConnection *ClientContext::get_connection(mac_id_t mac_id) {
    DLOG_ASSERT(mac_id != client_id, "Can't find self connection");
    if (mac_id == master_id) {
        return &master_connection;
    } else if (mac_id == local_rack_daemon_connection.daemon_id) {
        return &local_rack_daemon_connection;
    } else {
        ClientToDaemonConnection *ctx;
        bool ret = other_rack_daemon_connection.find(mac_id, &ctx);
        DLOG_ASSERT(ret, "Can't find mac %d", mac_id);
        return ctx;
    }
}