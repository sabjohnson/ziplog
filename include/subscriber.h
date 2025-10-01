#pragma once
#include "config.h"
#include "network_utils.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    class Subscriber {
        private:
            // base node
            ZiplogConfig config_;
            NodeId id_;
            string ip_address_;
            int port_;
            atomic<bool> is_running_;

            int subscriber_sock_;

            // quorum tracking
            unordered_map<SequenceNumber, set<NodeId>> pending_quorum_; // seq_number to set of server id. not reached quorum yet
            set<SequenceNumber> applied_;                               // seq_number had reached quorum (not necessarily added to log)

            // logging
            vector<Command> log_;
            SequenceNumber next_seq_;                                   // next expected seq_number in log
            unordered_map<SequenceNumber, Command> out_of_order_;       // reached quorum but not next expected seq_number

            // threading
            mutex mu_;
            thread running_thread_;

            void run();
            void handle_server(int server_sock);
            void process_for_quorum(const Message& msg);
            void apply_operation(const Message& msg);

        public:
            Subscriber(NodeId subscriber_id, const ZiplogConfig& cfg);
            void shutdown();
            ~Subscriber();
    };
}}
