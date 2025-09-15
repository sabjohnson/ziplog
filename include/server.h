#pragma once

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    class Server {
        int id;
        ziplogConfig config;
        string ipAddress;
        int port;
        bool isRunning;
        
        Server(int server_id, const ziplogConfig& cfg);
        void run();
        void shutdown();
    }
}}
