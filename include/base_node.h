#pragma once
#include "config.h"
#include "network_utils.h"
#include <unistd.h>
#include <sys/socket.h>  // accept()
#include <netinet/in.h>  // sockaddr_in
#include <condition_variable>

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    template <typename ConfigType> // https://www.geeksforgeeks.org/cpp/templates-cpp/
    class BaseNode {
    protected:
        ConfigType config_;
        atomic<bool> is_running_;
        int sock_;
        thread running_thread_;

        // track accepted connections
        vector<int> accepted_sockets_;
        mutex mu_;  // used for accepted connection

        BaseNode(const ConfigType& cfg)
            : config_(cfg)
            , is_running_(false)
            , sock_(-1)
        {}

        void start_listening() {
            running_thread_ = thread(&BaseNode::run, this);
        }

        virtual ~BaseNode() {
            shutdown();
            if (running_thread_.joinable()) {
                running_thread_.join();  // wait for thread to finish
            }
        }

    private:
        void run() {
            if (is_running_) {
                return;
            }

            // create listening socket
            sock_ = NetworkUtils::create_listening_socket(config_.address.ip, config_.address.port, true);
            if (sock_ < 0) {
                //std::cerr << "failed to create server socket" << std::endl;
                cerr << "failed to create server socket on " << config_.address.ip << ":" << config_.address.port
                    << " - " << strerror(errno) << std::endl;
                is_running_ = false;
                return;
            }

            // handle connections
            std::cout << "listening on " << config_.address.ip << ":" << config_.address.port << std::endl;
            is_running_ = true;
            while (is_running_) {
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);

                // accept connection (https://pubs.opengroup.org/onlinepubs/009604499/functions/accept.html)
                int client_socket = accept(sock_, (struct sockaddr*)&client_addr, &client_len);
                if (client_socket < 0) {
                    if (is_running_) {
                        //std::cerr  << "failed to accept connection" << std::endl;
                        continue;
                    } else {
                        break;
                    }
                }

                // track the socket
                {
                    lock_guard<mutex> lock(mu_);
                    accepted_sockets_.push_back(client_socket);
                }

                // handle connection in thread (https://en.cppreference.com/w/cpp/thread/thread/thread.html)
                thread([this, client_socket] () {
                    handle_connection(client_socket);
                    // stop tracking the socket
                    {
                        lock_guard<mutex> lock(mu_);
                        auto it = find(accepted_sockets_.begin(), accepted_sockets_.end(), client_socket);
                        if (it != accepted_sockets_.end()) {
                            accepted_sockets_.erase(it);
                        }
                    }
                }).detach();
            }
        }

        virtual void handle_connection(int client_socket) = 0;

    public:
        ShardId shard() const {
            return config_.shard;
        }

        NodeId id() const {
            return config_.id;
        }

        Address address() const {
            return config_.address;
        }

        size_t quorum() const {
            return config_.f + 1;
        }

        int f() const {
            return config_.f;
        }

        bool running() const {
            return is_running_;
        }

        virtual void shutdown() {
            is_running_ = false;
            if (sock_ >= 0) {
                ::shutdown(sock_, SHUT_RDWR);
                close(sock_);
                sock_ = -1;
            }
            // close all accepted sockets
            {
                lock_guard<mutex> lock(mu_);
                for (int sock : accepted_sockets_) {
                    ::shutdown(sock, SHUT_RDWR);
                    close(sock);
                }
                accepted_sockets_.clear();
            }
        }
    };
}}