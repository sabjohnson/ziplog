#pragma once
#include "config.h"
#include "network_utils.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    struct BatchRequest {
        NodeId proxy_id;
        vector<Timestamp> ordering_values;
        int proxy_socket;
    };

    class Zipper {
        private:
            // base node
            ZiplogConfig config_;
            ShardId shard_id_;
            string ip_address_;
            int port_;
            atomic<bool> is_running_;

            int zipper_sock_;

            // state for global sequence numbers (currently only using globalSeqNum)
            int num_proxies_;
            SequenceNumber global_seq_num_;
            unordered_map<NodeId, BatchRequest> pending_requests_;

            // epoch tracking
            Timestamp epoch_startup_;

            // threading
            mutex mu_;
            thread running_thread_;
            thread epoch_thread_;

            void run();
            void epoch_timer();                     // looping logic for epoch timer/slot allocation
            void allocate_slots();                  // sort timestamp and give global sequence numbers
            void handle_proxy(int proxy_socket);    // returns sequence numbers

        public:
            Zipper(const ZiplogConfig& cfg);
            void shutdown();
            ~Zipper();
    };

}}
