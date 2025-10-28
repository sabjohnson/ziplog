#pragma once
#include "base_node.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    class Subscriber : public BaseNode {
        private:
            // quorum tracking
            unordered_map<SequenceNumber, set<NodeId>> pending_quorum_; // seq_number to set of server id. not reached quorum yet
            set<SequenceNumber> applied_;                               // seq_number had reached quorum (not necessarily added to log)

            // logging
            vector<Command> log_;
            SequenceNumber next_seq_;                                   // next expected seq_number in log
            unordered_map<SequenceNumber, Command> out_of_order_;       // reached quorum but not next expected seq_number

            // threading safety
            mutex mu_;

            void handle_connection(int server_sock) override;
            void process_for_quorum(const Message& msg);
            void apply_operation(const Message& msg);

        public:
            Subscriber(NodeId subscriber_id, const ZiplogConfig& cfg);
            void shutdown() override;
            ~Subscriber();

            void print_log();
            void print_expanded_log();
            vector<Command> log() {
                return log_;
            }
    };
}}
