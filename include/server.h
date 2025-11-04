#pragma once
#include "base_node.h"
#include <deque>

using namespace ziplog::api;
using std::deque;

namespace ziplog {
namespace impl {

    // Storage servers in the paper
    class Server : public BaseNode {
    private:
        // threading safety
        mutex mu_;
        atomic<bool> running_;
        thread failure_detector_thread_;

        Timestamp lag_;
        unordered_map<NodeId, bool> blocked_for_reconfiguration_;
        unordered_map<NodeId, deque<SequenceNumber>> proxy_timeouts_;
        unordered_map<NodeId, SequenceNumber> last_used_sequence_number_;

        void handle_connection(int proxy_socket) override;
        void handle_freeze(int proxy_socket, const Message& msg);
        void broadcast_to_subscribers(const Message& msg);
        void update_expected_proxy_timeouts(const Message& msg);
        void block_proxy(const Message& msg);
        bool is_blocked(NodeId id);
        void failure_detect();
        void report(NodeId id);
        void remove_timeout(NodeId id);

       void start_proxy_liveness_checks() {
            failure_detector_thread_ = thread(&Server::failure_detect, this);
       }

    public:
        Server(NodeId server_id, const ZiplogConfig& cfg);
        void shutdown() override;
        ~Server();
    };
}}
