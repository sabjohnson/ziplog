#pragma once
#include "network_utils.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    class ConnectionPool {
    private:
        struct SocketState {
            int socket;
            unique_ptr<mutex> mu;   // one mutex per socket

            SocketState(int s) : socket(s), mu(make_unique<mutex>()) {}
        };
        unordered_map<Address, shared_ptr<SocketState>> active_connections_;  // address struct -> socket
        mutex mu_;

    public:
        int get_connection(const Address& addr) {
            lock_guard<mutex> lock(mu_);

            if (active_connections_.find(addr) != active_connections_.end()) {
                int sock = active_connections_[addr];  // attempt to reuse existing

                if (socket_alive(sock)) {
                    return sock;
                } else {
                    close(sock);
                    active_connections_.erase(addr);
                }
            }

            // create new persistent connection
            int sock = NetworkUtils::create_connector_socket();
            if (sock< 0) return -1;

            if (!NetworkUtils::connect_to_address(sock, addr.ip, addr.port)) {
                close(sock);
                return -1;
            }

            active_connections_[addr] = sock;
            return sock;
        }

        // overload to allow coompilation during migration
        int get_connection(const string& ip, int port) {
            return get_connection(Address(ip, port));
        }

        void close_connection(const Address& addr) {
            lock_guard<mutex> lock(mu_);
            auto it = active_connections_.find(addr);
            if (it != active_connections_.end()) {
                close(it->second);
                active_connections_.erase(it);
            }
        }

        void close_all() {
            lock_guard<mutex> lock(mu_);
            for (auto& [addr, sock] : active_connections_) {
                close(sock);
            }
            active_connections_.clear();
        }

        ~ConnectionPool() {
            close_all();
        }

    private:
        bool socket_alive(int sock) {
            char buf;
            int result = recv(sock, &buf, 1, MSG_PEEK | MSG_DONTWAIT);
            if (result == 0) return false;  // connection closed
            if (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                return false;  // error
            }
            return true;
        }
    };
}}