#pragma once
#include "base_node.h"
#include "connection_pool.h"
#include <condition_variable>

using namespace ziplog::api;
using std::condition_variable;
namespace ziplog {
namespace impl {

    class Subscriber : public BaseNode<SubscriberConfig> {
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
            condition_variable log_cv_;

            // connection pool optimization
            ConnectionPool connection_pool_;

            void handle_connection(int server_sock) override;
            void process_for_quorum(const Message& msg);
            void apply_operation(const Message& msg);

        public:
            Subscriber(const SubscriberConfig& cfg);
            Subscriber(const SubscriberConfig& cfg, bool registered);
            void shutdown() override;
            ~Subscriber();

            void print_log();
            void print_expanded_log();
            vector<Command> log() {
                return log_;
            }

            void wait_for_log_size(int size) {
                std::unique_lock<mutex> lock(mu_);
                log_cv_.wait(lock, [this, size]() {
                    return static_cast<int>(log_.size()) > size;
                });
            }
    };
}}
