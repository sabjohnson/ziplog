#pragma once
#include "base_node.h"
#include <future>
#include <deque>

using namespace ziplog::api;
using std::deque;

namespace ziplog {
namespace impl {

    struct BatchRequest {
        NodeId proxy_id;
        vector<Timestamp> ordering_values;
        int proxy_socket;
    };

    class Zipper: public BaseNode<ZipperConfig> {
        private:
            // state for global sequence numbers (currently only using globalSeqNum)
            SequenceNumber global_seq_num_;
            unordered_map<NodeId, bool> blocked_for_reconfiguration_;   // false = in process of blocking, true = reconfiguration complete
            unordered_map<NodeId, SequenceNumber> rounds_;
            unordered_map<NodeId, set<NodeId>> reported_proxies_;
            unordered_map<NodeId, SequenceNumber> proxy_last_sequence_;
            unordered_map<NodeId, SequenceNumber> proxy_estimates_;
            unordered_map<NodeId, vector<SequenceNumber>> proxy_allocated_sequences_;

            // reconfiguration
            deque<pair<string, int>> joining_proxies_;

            // epoch tracking
            Timestamp epoch_startup_;
            Timestamp next_epoch_;

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
            void request_last_messages(Message &msg);
            void allocate_slots();                  // sort timestamp and give global sequence numbers
            void deliver_slot_allocation(NodeId proxy_id, const vector<SequenceNumber>& values);
            void add_proxy(const Message& msg, bool is_new);
            void introduce_proxies();
            void introduce_subscriber(const Message& msg);

        public:
            Zipper(const ZipperConfig& cfg);
            void shutdown() override;
            ~Zipper();

            size_t num_proxies() const {
                return config_.proxies.size();
            }

            size_t num_servers() const {
                return config_.servers.size();
            }

            size_t num_subscribers() const {
                return config_.subscribers.size();
            }
    };

}}
