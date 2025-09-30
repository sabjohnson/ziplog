#pragma once
#include "config.h"
#include "network_utils.h"
#include <atomic>
#include <thread>

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    // Storage servers in the paper
    class Server {
        private:
            ziplogConfig config;
            int shard_id;
            int id;
            string ipAddress;
            int port;
            std::atomic<bool> isRunning;
            int server_sock;
            std::thread running_thread;
            
            void handleProxy(int proxy_socket);
            void handleAppendMessage(const message& msg);
        
        public:
            Server(int server_id, const ziplogConfig& cfg);
            void run();
            void shutdown();
            ~Server();
    };
}}
