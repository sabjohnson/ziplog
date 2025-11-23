#pragma once
#include "base_node.h"
#include <deque>

using namespace ziplog::api;
using std::deque;

namespace ziplog {
namespace impl {

    // Storage servers in the paper
    class Server : public BaseNode<ServerConfig> {
    private:
        unordered_map<NodeId, deque<Message>> proxy_messages_;

        // threading safety
        mutex mu_;
        atomic<bool> running_;
        thread failure_detector_thread_;

        Timestamp lag_;
        unordered_map<NodeId, bool> blocked_for_reconfiguration_;   // false = in process of blocking, true = reconfiguration complete
        unordered_map<NodeId, SequenceNumber> rounds_;              // current round of freeze
        unordered_map<NodeId, set<NodeId>> rounds_responders_;      // other servers i heard from for this round (transfer request)

        unordered_map<NodeId, deque<SequenceNumber>> proxy_timeouts_;
        unordered_map<NodeId, SequenceNumber> last_used_sequence_number_;

        void handle_connection(int proxy_socket) override;
        void handle_freeze(const Message& msg, bool from_zipper);
        void handle_transfer_request(int socket, const Message& msg);
        void broadcast_to_subscribers(const Message& msg);
        void update_expected_proxy_timeouts(const Message& msg);
        void block_proxy(const Message& msg);
        bool is_blocked(NodeId id);
        void failure_detect();
        void report(NodeId id);
        void remove_timeout(const Message& msg);
        void introduce_proxy(const Message& msg);

       void start_proxy_liveness_checks() {
            failure_detector_thread_ = thread(&Server::failure_detect, this);
       }

    public:
        Server(const ServerConfig& cfg);
        void shutdown() override;
        ~Server();

        size_t num_proxies() {
            return config_.proxies.size();
        }

        size_t num_subscribers() {
            return config_.subscribers->size();
        }
    };
}}
