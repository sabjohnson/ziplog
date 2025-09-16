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
        if (client_id < 0 || static_cast<size_t>(client_id) >= cfg.clients.size()) {
            throw std::invalid_argument("Id " + std::to_string(client_id) + " not in range of " + std::to_string(cfg.clients.size()));
        }
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        id = client_id;
        config = cfg;
        auto [ip, p] = cfg.clients[client_id];
        ipAddress = ip;
        port = p;
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
            bool success = false;
            
            // set up server addr
            struct sockaddr_in server_addr;
            memset(&server_addr, 0, sizeof(server_addr));
            server_addr.sin_family = AF_INET;
            server_addr.sin_port = htons(server_port);
            inet_pton(AF_INET, server_ip.c_str(), &server_addr.sin_addr);
            
            for (int attempt = 0; attempt < config.max_retries && !success; attempt++) {
                // bind to socket
                int sockfd = socket(AF_INET, SOCK_STREAM, 0);   // creates socket
                if (sockfd < 0) continue;
                
                // set receive timeout
                struct timeval timeout;
                timeout.tv_sec = config.timeout_ms / 1000;
                timeout.tv_usec = (config.timeout_ms % 1000) * 1000;
                setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
                
                if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
                    std::cerr << "Failed to connect to server " << server_ip << ":" << server_port << std::endl;
                    close(sockfd);
                    continue;
                }
    
                if (NetworkUtils::sendMessage(sockfd, msg)) {
                    message ack_response;
                    if (NetworkUtils::recvMessage(sockfd, ack_response)) {
                        if (ack_response.type == ACK && ack_response.sequence_number == msg.sequence_number) {
                            success = true;
                            std::cout << "Received ACK from server " << i << std::endl;
                        }
                    }
                }
                close(sockfd);
            }
            if (success) successful_sends++;
        }
        // increment operation number
        operation_number++;
        return successful_sends == static_cast<int>(config.servers.size());
    }
    
    void Client::shutdown() {
        isRunning = false;
    }
}}
