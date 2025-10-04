#pragma once
#include "base_node.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    struct BatchRequest {
        NodeId proxy_id;
        vector<Timestamp> ordering_values;
        int proxy_socket;
    };

    class Zipper: public BaseNode {
        private:
            // state for global sequence numbers (currently only using globalSeqNum)
            size_t num_proxies_;
            SequenceNumber global_seq_num_;
            unordered_map<NodeId, SequenceNumber> proxy_estimates_;

            // epoch tracking
            Timestamp epoch_startup_;

            // threading
            mutex mu_;
            atomic<bool> epoch_running_;
            thread epoch_thread_;

            void start_epochs() {
                epoch_thread_ = thread(&Zipper::epoch_timer, this);  // inits epoch_startup_
            }

            void handle_connection(int proxy_socket) override;    // returns sequence numbers
            void epoch_timer();                     // looping logic for epoch timer/slot allocation
            void update_slot_estimate(Message &msg);           // takes note of a proxies requested number of slots
            void allocate_slots();                  // sort timestamp and give global sequence numbers

        public:
            Zipper(const ZiplogConfig& cfg);
            void shutdown() override;
            ~Zipper();
    };

}}
