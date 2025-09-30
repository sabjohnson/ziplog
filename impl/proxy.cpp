#include "proxy.h"
#include "network_utils.h"
#include <sys/socket.h>     // socket(), connect(), send(), recv()
#include <netinet/in.h>     // sockaddr_in, AF_INET
#include <arpa/inet.h>      // inet_pton(), htons(), ntohs()
#include <unistd.h>         // close()
#include <iostream>         // std::cout, std::cerr
#include <cstring>          // memset()

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Proxy::Proxy(int proxy_id, const ziplogConfig &cfg) {
        if (proxy_id < 0 || static_cast<size_t>(proxy_id) >= cfg.proxies.size()) {
            throw std::invalid_argument("Id " + std::to_string(proxy_id) + " not in range of " + std::to_string(cfg.proxies.size()));
        }
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        config = cfg;
        shard_id = 0;
        id = proxy_id;
        auto [ip, p] = cfg.proxies[proxy_id];
        ipAddress = ip;
        port = p;
        isRunning = true;
    }

    bool Proxy::append(const std::string &data) {
        // get seq number from zipper !!
        message zip_req;
        zip_req.type = ZIP_REQUEST;
        zip_req.shard_id = shard_id;
        zip_req.sender_id = id;

        message zip_resp;
        if (!NetworkUtils::requestFromZipper(config.zipper.first, config.zipper.second, zip_req, zip_resp, config.timeout_ms, config.max_retries)) {
            return false;
        }

        if (zip_resp.get_assigned_sequences().empty()) {
            std::cerr << "Zipper returned no sequence numbers" << std::endl;
            return false;
        }

        // create msg to be broadcasted
        message msg;
        msg.type = APPEND;
        msg.shard_id = shard_id;
        msg.sender_id = id;
        msg.seq_or_count = zip_resp.get_assigned_sequences()[0];
        msg.data = data;
        
        // attempt to replicate on f + 1 storage servers
        int successful_sends = 0;
        for (size_t i = 0; i < config.servers.size() && successful_sends < config.f + 1; i++) {
            auto [server_ip, server_port] = config.servers[i];

            if (NetworkUtils::sendMessageToAddress(server_ip, server_port, msg, config.timeout_ms, config.max_retries)) {
                successful_sends++;
                std::cout << "Received ACK from server " << i << std::endl;
            }
        }
        return successful_sends == static_cast<int>(config.f + 1);
    }
    
    void Proxy::shutdown() {
        isRunning = false;
    }
}}
