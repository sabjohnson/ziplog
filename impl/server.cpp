#include "server.h"
#include "network_utils.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <iostream>

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
        shard_id = 0;
        id = server_id;
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
        
        // create listening socket
        server_sock = NetworkUtils::createListeningSocket(ipAddress, port, true);
        if (server_sock < 0) {
            std::cerr << "Server " << std::to_string(id) << " failed to create server socket" << std::endl;
            return;
        }

        // handle connections
        std::cout << "Server " << id << " listening on " << ipAddress << ":" << port << std::endl;
        isRunning = true;
        while (isRunning) {
            struct sockaddr_in proxy_addr;
            socklen_t proxy_len = sizeof(proxy_addr);
            
            // accept connection (https://pubs.opengroup.org/onlinepubs/009604499/functions/accept.html)
            int proxy_socket = accept(server_sock, (struct sockaddr*)&proxy_addr, &proxy_len);
            if (proxy_socket < 0) {
                if (isRunning) {
                    std::cerr << "Failed to accept connection" << std::endl;
                    continue;
                } else {
                    break;
                }
            }
            
            // handle proxy connection in thread (https://en.cppreference.com/w/cpp/thread/thread/thread.html)
            std::thread proxy_thread(&Server::handleProxy, this, proxy_socket);
            proxy_thread.detach();
        }
    }
    
    void Server::handleProxy(int proxy_socket) {
        while (isRunning) {
            // recv message from proxy
            message msg;
            if (!NetworkUtils::recvMessage(proxy_socket, msg)) {
                //std::cerr << "Failed to receive message from proxy" << std::endl;
                break;
            }
            std::cout << "Server " << id << " received message from proxy " << msg.sender_id
                      << " (seq: " << msg.seq_or_count << ", type: " << msg.type << ")" << std::endl;

            // verify validity of sender
            if (msg.sender_id >= static_cast<uint32_t>(config.proxies.size()) || msg.shard_id != static_cast<uint32_t>(shard_id)) {
                break;
            }

            // process message
            if (msg.type == APPEND) {
                handleAppendMessage(msg);
            }
            
            // send response (default to ack for now)
            message ack_msg;
            ack_msg.type = ACK;
            ack_msg.shard_id = shard_id;
            ack_msg.sender_id = id;
            ack_msg.seq_or_count = msg.seq_or_count;
            ack_msg.data = "gotchu!";   // not necessary
            
            if (!NetworkUtils::sendMessage(proxy_socket, ack_msg)) {
                std::cerr << "Failed to send ACK to proxy" << std::endl;
                // break here?
            } else {
                std::cout << "Server " << id << " sent ACK for message " << msg.seq_or_count << "\n" << std::endl;
            }
        }
        close(proxy_socket);
    }
    
    // forwards request to all subscribers
    void Server::handleAppendMessage(const message &msg) {
        message fwd_msg = msg;
        fwd_msg.sender_id = id;

        for (size_t i = 0; i < config.subscribers.size(); i++) {
            auto [subscriber_ip, subscriber_port] = config.subscribers[i];

            if (!NetworkUtils::sendMessageToAddress(subscriber_ip, subscriber_port, fwd_msg, config.timeout_ms, config.max_retries)) {
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
