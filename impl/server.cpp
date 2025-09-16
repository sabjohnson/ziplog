#include "server.h"
#include "network_utils.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <cstring>

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Server::Server(int server_id, const ziplogConfig &cfg) {
        if (server_id < 0 || static_cast<size_t>(server_id) >= cfg.servers.size()) {
            throw std::invalid_argument("Id " + std::to_string(server_id) + " not in range of " + std::to_string(cfg.servers.size()));
        }
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        id = server_id;
        config = cfg;
        auto [ip, p] = cfg.servers[server_id];
        ipAddress = ip;
        port = p;
        isRunning = false;
        server_sock = -1;
        running_thread = std::thread(&Server::run, this);
    }
    
    void Server::run() {
        if (isRunning) {
            std::cerr << "Server " << std::to_string(id) << " already running" << std::endl;
            return;
        }
        
        // create socket server
        server_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (server_sock < 0) {
            std::cerr << "Server " << std::to_string(id) << " failed to create server socket" << std::endl;
            return;
        }
        
        // set sock option to allow re-use (https://pubs.opengroup.org/onlinepubs/009695099/functions/setsockopt.html)
        int enabled = 1;
        if (setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) < 0) {
            std::cerr << "Server " << std::to_string(id) << " failed to set server socket options" << std::endl;
            return;
        }
        
        // bind socket to address
        struct sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);
        inet_pton(AF_INET, ipAddress.c_str(), &server_addr.sin_addr);
        
        if (bind(server_sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            std::cerr << "Failed to bind socket to " << ipAddress << ":" << port << std::endl;
            close(server_sock);
            return;
        }
        
        // listen for connections (https://man7.org/linux/man-pages/man2/listen.2.html)
        if (listen(server_sock, 10) < 0) {
            std::cerr << "Failed to listen on socket" << std::endl;
            close(server_sock);
            return;
        }
        
        // handle connections
        std::cout << "Server " << id << " listening on " << ipAddress << ":" << port << std::endl;
        isRunning = true;
        while (isRunning) {
            struct sockaddr_in client_addr;
            socklen_t client_len = sizeof (client_addr);
            
            // accept connection (https://pubs.opengroup.org/onlinepubs/009604499/functions/accept.html)
            int client_socket = accept(server_sock, (struct sockaddr*)&client_addr, &client_len);
            if (client_socket < 0) {
                if (isRunning) {
                    std::cerr << "Failed to accept connection" << std::endl;
                    continue;
                } else {
                    break;
                }
            }
            
            // handle client connection in thread (https://en.cppreference.com/w/cpp/thread/thread/thread.html)
            std::thread client_thread(&Server::handleClient, this, client_socket);
            client_thread.detach();
        }
        
        // close socket server
        close(server_sock);
    }
    
    void Server::handleClient(int client_socket) {
        while (isRunning) {
            // recv message from client
            message msg;
            if (!NetworkUtils::recvMessage(client_socket, msg)) {
                //std::cerr << "Failed to receive message from client" << std::endl;
                break;
            }
            std::cout << "Server " << id << " received message from client " << msg.sender_id
                      << " (seq: " << msg.sequence_number << ", type: " << msg.type << ")" << std::endl;
                      
            // process message
            if (msg.type == APPEND) {
                handleAppendMessage(msg);
            }
            
            // send response (default to ack for now)
            message ack_msg;
            ack_msg.type = ACK;
            ack_msg.sender_id = id;
            ack_msg.sequence_number = msg.sequence_number;
            ack_msg.data = "gotchu!";
            
            if (!NetworkUtils::sendMessage(client_socket, ack_msg)) {
                std::cerr << "Failed to send ACK to client" << std::endl;
                // break here?
            } else {
                std::cout << "Server " << id << " sent ACK for message " << msg.sequence_number << "\n" << std::endl;
            }
        }
        close(client_socket);
    }
    
    // forwards request to all subscribers
    void Server::handleAppendMessage(const message &msg) {
        for (size_t i = 0; i < config.subscribers.size(); i++) {
            auto [subscriber_ip, subscriber_port] = config.subscribers[i];
            bool success = false;
            
            struct sockaddr_in subscriber_addr;
            memset(&subscriber_addr, 0, sizeof(subscriber_addr));
            subscriber_addr.sin_family = AF_INET;
            subscriber_addr.sin_port = htons(subscriber_port);
            inet_pton(AF_INET, subscriber_ip.c_str(), &subscriber_addr.sin_addr);
            
            for (int attempt = 0; attempt < config.max_retries && !success; attempt++) {
                // connect to subscriber
                int subscriber_socket = socket(AF_INET, SOCK_STREAM, 0);
                if (subscriber_socket < 0) {
                    std::cerr << "Failed to create socket for subscriber " << i << std::endl;
                    continue;
                }
                // set timeout
                struct timeval timeout;
                timeout.tv_sec = config.timeout_ms / 1000;
                timeout.tv_usec = (config.timeout_ms % 1000) * 1000;
                setsockopt(subscriber_socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
                
                if (connect(subscriber_socket, (struct sockaddr*)&subscriber_addr, sizeof(subscriber_addr)) < 0) {
                    std::cerr << "Failed to connect to subscriber " << subscriber_ip << ":" << subscriber_port << std::endl;
                    close(subscriber_socket);
                    continue;
                }
                
                // fwd message with this servers id as sender
                message fwd_msg = msg;
                fwd_msg.sender_id = id;
                
                if (NetworkUtils::sendMessage(subscriber_socket, fwd_msg)) {
                    std::cout << "Server " << id << " forwarded message to subscriber " << i << std::endl;
                    message ack_response;
                    if (NetworkUtils::recvMessage(subscriber_socket, ack_response)) {
                        if (ack_response.type == ACK && ack_response.sequence_number == msg.sequence_number) {
                            std::cout << "Server " << id << " received ACK from subscriber " << i << std::endl;
                            success = true;
                        }
                    }
                }
                close(subscriber_socket);
            }
            if (!success) {
                std::cerr << "Server " << id << " failed to deliver to subscriber " << i << " after " << config.max_retries << " attempts" << std::endl;
            }
        }
    }
    
    void Server::shutdown() {
        isRunning = false;
        if (server_sock >= 0) {
            ::shutdown(server_sock, SHUT_RDWR);
            close(server_sock);
            server_sock = -1;
        }
        std::cout << "Server " << id << " shutting down" << std::endl;
    }
    
    Server::~Server() {
        shutdown();
        if (running_thread.joinable()) {
            running_thread.join();  // wait for thread to finish
        }
    }
}}
