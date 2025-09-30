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
            ziplogConfig config;
            int id;
            string ipAddress;
            int port;
            std::atomic<bool> isRunning;
            int subscriber_sock;
            std::thread running_thread;
            std::mutex mutex;

            vector<string> log;
            std::set<uint64_t> applied;         // seq_number had reached quorum and added to log
            uint64_t next_seq;                  // next expected seq_number in log
            std::map<uint64_t, string> gaps;    // reached quorum but not next expected seq_number
            std::map<uint64_t, std::set<uint32_t>> pending; // seq_number to set of server id. not reached quorum yet

            void handleServer(int server_sock);
            void processForQuorum(const message& msg);
            void applyOperation(const message& msg);

        public:
            Subscriber(int subscriber_id, const ziplogConfig& cfg);
            void run();
            void shutdown();
            ~Subscriber();
    };
}}
