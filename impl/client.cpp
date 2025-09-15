#include "client.h"
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

    Client::Client(int client_id, const ziplogConfig &cfg) {
        if (static_cast<size_t>(client_id) >= cfg.clients.size()) {
            throw std::invalid_argument("Id " + std::to_string(client_id) + " not in range of " + std::to_string(cfg.clients.size()));
        }
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        id = client_id;
        config = cfg;
        auto [ipAddress, port] = cfg.clients[id];
        isRunning = true;
        operation_number = 0;
    }

    bool Client::append(const std::string &data) {
        // create msg to be broadcasted
        message msg;
        msg.type = APPEND;
        msg.sender_id = id;
        msg.sequence_number = operation_number * config.clients.size() + id;
        msg.data = data;
        
        // attempt to broadcast
        int successful_sends = 0;
        for (size_t i = 0; i < config.servers.size(); i++) {
            auto [server_ip, server_port] = config.servers[i];
            
            // bind to socket
            int sockfd = socket(AF_INET, SOCK_STREAM, 0);   // creates socket
            if (sockfd < 0) {
                continue;
            }
            struct sockaddr_in server_addr;
            memset(&server_addr, 0, sizeof(server_addr));
            server_addr.sin_family = AF_INET;
            server_addr.sin_port = htons(server_port);
            inet_pton(AF_INET, server_ip.c_str(), &server_addr.sin_addr);
            
            if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
                std::cerr << "Failed to connect to server " << server_ip << ":" << server_port << std::endl;
                close(sockfd);
                continue;
            }

            if (NetworkUtils::sendMessage(sockfd, msg)) {
                successful_sends++;
                std::cout << "Sent to server " << i << std::endl;
            } else {
                std::cerr << "Failed to send to server " << i << std::endl;
            }
            close(sockfd);
        }
        // increment operation number
        operation_number++;
        return successful_sends == static_cast<int>(config.servers.size());
    }
    
    void Client::shutdown() {
        isRunning = false;
    }
}}
