#include "impl.hpp"

ClientConnection *ClientContext::get_connection(mac_id_t mac_id) {
    DLOG_ASSERT(mac_id != m_client_id, "Can't find self connection");
    if (mac_id == master_id) {
        return &m_master_connection;
    } else if (mac_id == m_local_rack_daemon_connection.daemon_id) {
        return &m_local_rack_daemon_connection;
    } else {
        ClientToDaemonConnection *ctx;
        bool ret = m_other_rack_daemon_connection.find(mac_id, &ctx);
        DLOG_ASSERT(ret, "Can't find mac %d", mac_id);
        return ctx;
    }
}