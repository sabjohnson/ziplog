#pragma once
#include "config.h"
#include "network_utils.h"
#include <atomic>
#include <thread>
#include <mutex>
#include <map>
#include <set>

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    class Subscriber {
        private:
            int id;
            ziplogConfig config;
            string ipAddress;
            int port;
            std::atomic<bool> isRunning;
            int subscriber_sock;
            std::thread running_thread;
            
            std::mutex mutex;
            std::map<uint32_t, std::set<int>> pending; // key: sequence number, value: set of servers that have sent this append
            std::set<uint32_t> applied;
            vector<string> log;
            
            void handleServer(int server_sock);
            void processForQuorum(const message& msg);
            void applyOperation(const message& msg);
            
        public:
            Subscriber(int subscriber_id, ziplogConfig& cfg);
            void run();
            void shutdown();
            ~Subscriber();
    };
}}
