#pragma once
#include "config.h"
#include "network_utils.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    // Storage servers in the paper
    class Server {
        private:
            // base node
            ZiplogConfig config_;
            ShardId shard_id_;
            NodeId id_;
            string ip_address_;
            int port_;
            atomic<bool> is_running_;

            int server_sock_;

            // threading
            thread running_thread_;

            void run();
            void handle_proxy(int proxy_socket);
            void handle_append_message(const Message& msg);
        
        public:
            Server(NodeId server_id, const ZiplogConfig& cfg);
            void shutdown();
            ~Server();
    };
}}
