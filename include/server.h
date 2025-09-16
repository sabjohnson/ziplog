#pragma once
#include "config.h"
#include "network_utils.h"
#include <atomic>
#include <thread>

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    class Server {
        private:
            int id;
            ziplogConfig config;
            string ipAddress;
            int port;
            std::atomic<bool> isRunning;
            int server_sock;
            std::thread running_thread;
            
            void handleClient(int client_socket);
            void handleAppendMessage(const message& msg);
        
        public:
            Server(int server_id, const ziplogConfig& cfg);
            void run();
            void shutdown();
            ~Server();
    };
}}
