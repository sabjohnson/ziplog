#pragma once
#include "config.h"
#include "network_utils.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    class Server {
        private:
            int id;
            ziplogConfig config;
            string ipAddress;
            int port;
            bool isRunning;
            int server_sock;
            
            void handleClient(int client_socket);
            void handleAppendMessage(const message& msg);
        
        public:
            Server(int server_id, const ziplogConfig& cfg);
            void run();
            void shutdown();
    };
}}
