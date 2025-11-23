#include "network_utils.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    class ConnectionPool {
        unordered_map<string, int> active_connections_;  // "ip:port" -> socket
        mutex mu_;

    public:
        int get_connection(const string& ip, int port) {
            lock_guard<mutex> lock(mu_);
            string key = ip + ":" + to_string(port);

            if (active_connections_.find(key) != active_connections_.end()) {
                return active_connections_[key];  // reuse existing
            }

            // create new persistent connection
            int sock = NetworkUtils::create_connector_socket();
            NetworkUtils::connect_to_address(sock, ip, port);
            active_connections_[key] = sock;
            return sock;
        }

        void close_all() {
            for (auto& [key, sock] : active_connections_) {
                close(sock);
            }
        }
    };
}}