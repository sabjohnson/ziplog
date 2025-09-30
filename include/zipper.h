#pragma once
#include "config.h"
#include "network_utils.h"
#include <atomic>
#include <thread>
#include <mutex>
#include <chrono>

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    struct batchRequest {
        uint32_t proxy_id;
        uint64_t num_slots;
        int proxy_socket;
    };

    class Zipper {
        private:
            ziplogConfig config;
            int shard_id;
            string ipAddress;
            int port;
            std::atomic<bool> isRunning;
            int zipper_sock;
            std::thread running_thread;

            // state for global sequence numbers (currently only using globalSeqNum)
            int numProxies;
            uint64_t globalSeqNum;
            vector<batchRequest> pending_requests;

            // epoch tracking
            std::chrono::time_point<std::chrono::system_clock> epoch_startup;
            std::thread epoch_thread;

            void epochTimer();      // looping logic for epoch timer/slot allocation
            void allocateSlots();   // sort timestamp and give global sequence numbers

            void handleProxy(int proxy_socket); // returns sequence numbers
            std::mutex counter_mutex;

        public:
            Zipper(const ziplogConfig& cfg);
            void run();
            void shutdown();
            ~Zipper();
    };

}}
