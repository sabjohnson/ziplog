#include "server.h"
#include "network_utils.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Server::Server(NodeId server_id, const ZiplogConfig &cfg) {
        // validate node id
        validate_node_id(server_id, cfg.num_servers(), "Server");
        
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        config_ = cfg;
        shard_id_ = 0;
        id_ = server_id;
        auto [ip, p] = cfg.servers[server_id];
        ip_address_ = ip;
        port_ = p;
        is_running_ = false;
        server_sock_ = -1;
        running_thread_ = thread(&Server::run, this);
    }
    
    void Server::run() {
        if (is_running_) {
            std::cerr << "Server " << std::to_string(id_) << " already running" << std::endl;
            return;
        }
        
        // create listening socket
        server_sock_ = NetworkUtils::create_listening_socket(ip_address_, port_, true);
        if (server_sock_ < 0) {
            std::cerr << "Server " << std::to_string(id_) << " failed to create server socket" << std::endl;
            return;
        }

        // handle connections
        std::cout << "Server " << id_ << " listening on " << ip_address_ << ":" << port_ << std::endl;
        is_running_ = true;
        while (is_running_) {
            struct sockaddr_in proxy_addr;
            socklen_t proxy_len = sizeof(proxy_addr);
            
            // accept connection (https://pubs.opengroup.org/onlinepubs/009604499/functions/accept.html)
            int proxy_socket = accept(server_sock_, (struct sockaddr*)&proxy_addr, &proxy_len);
            if (proxy_socket < 0) {
                if (is_running_) {
                    std::cerr << "Failed to accept connection" << std::endl;
                    continue;
                } else {
                    break;
                }
            }
            
            // handle proxy connection in thread (https://en.cppreference.com/w/cpp/thread/thread/thread.html)
            thread proxy_thread(&Server::handle_proxy, this, proxy_socket);
            proxy_thread.detach();
        }
    }
    
    void Server::handle_proxy(int proxy_socket) {
        while (is_running_) {
            // recv message from proxy
            Message msg;
            if (!NetworkUtils::recv_message(proxy_socket, msg)) {
                //std::cerr << "Failed to receive message from proxy" << std::endl;
                break;
            }
            std::cout << "Server " << id_ << " received message from proxy " << msg.sender_id
                      << " (seq: " << msg.seq_or_count << ", type: " << msg.type << ")" << std::endl;

            // verify validity of sender
            if (msg.sender_id >= static_cast<NodeId>(config_.num_proxies()) || msg.shard_id != shard_id_) {
                break;
            }

            // process message
            if (msg.type == APPEND) {
                handle_append_message(msg);
            }
            
            // send response (default to ack for now)
            Message ack_msg;
            ack_msg.type = ACK;
            ack_msg.shard_id = shard_id_;
            ack_msg.sender_id = id_;
            ack_msg.seq_or_count = msg.seq_or_count;
            
            if (!NetworkUtils::send_message(proxy_socket, ack_msg)) {
                std::cerr << "Failed to send ACK to proxy" << std::endl;
                // break here?
            } else {
                std::cout << "Server " << id_ << " sent ACK for message " << msg.seq_or_count << "\n" << std::endl;
            }
        }
        close(proxy_socket);
    }
    
    // forwards request to all subscribers
    void Server::handle_append_message(const Message &msg) {
        Message fwd_msg = msg;
        fwd_msg.sender_id = id_;

        for (size_t i = 0; i < config_.num_subscribers(); i++) {
            auto [subscriber_ip, subscriber_port] = config_.subscribers[i];

            if (!NetworkUtils::send_message_to_address(subscriber_ip, subscriber_port, fwd_msg, config_.timeout_ms, config_.max_retries)) {
                std::cerr << "Server " << id_ << " failed to deliver to subscriber " << i << " after " << config_.max_retries << " attempts" << std::endl;
            }
        }
    }
    
    void Server::shutdown() {
        is_running_ = false;
        if (server_sock_ >= 0) {
            ::shutdown(server_sock_, SHUT_RDWR);
            close(server_sock_);
            server_sock_ = -1;
        }
        std::cout << "Server " << id_ << " shutting down" << std::endl;
    }
    
    Server::~Server() {
        shutdown();
        if (running_thread_.joinable()) {
            running_thread_.join();  // wait for thread to finish
        }
    }
}}
