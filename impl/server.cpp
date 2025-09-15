#include "server.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Server::Server(int server_id, const int &cfg) {
        if (static_cast<size_t>(server_id) >= cfg.servers.size()) {
            throw std::invalid_argument("Id " + std::to_string(client_id) + " not in range of " + std::to_string(cfg.clients.size()));
        }
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        id = server_id;
        config = cfg;
        auto [ipAddress, port] = cfg.servers[i];
        isRunning = false;
    }
    
    void Server::run() {}
    
    void Server::shutdown() {
        isRunning = false;
    }
    
    
}}
