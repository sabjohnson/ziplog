#pragma once
#include "base_node.h"
#include "connection_pool.h"
#include <condition_variable>
#include <deque>

using namespace ziplog::api;
using std::condition_variable;
using std::deque;

namespace ziplog {
namespace impl {

    // Storage servers in the paper
    class Server : public BaseNode<ServerConfig> {
    private:
        struct SubscriberWorker {
           std::unique_ptr<thread> worker_thread;
           int socket;
           mutex mu;
           condition_variable cv;

           optional<Message> pending_msg;
           bool has_work = false;
           bool shutdown = false;

           // result signaling
           atomic<bool> ack_received{false};
       };

       std::unordered_map<int, std::unique_ptr<SubscriberWorker>> subscriber_workers_;
       mutex subscriber_workers_mu_;

       mutex sub_ack_mu_;
       condition_variable sub_ack_cv_;
       atomic<int> sub_ack_count_{0};

       void subscriber_worker_loop(int subscriber_idx) {
            Address subscriber = config_.subscribers[subscriber_idx];
            SubscriberWorker& worker = *subscriber_workers_[subscriber_idx];

            // create persistent connection
            worker.socket = NetworkUtils::connect_to_address_persistent(subscriber.ip, subscriber.port);
            if (worker.socket < 0) {
                return;
            }

            while (true) {
                optional<Message> msg;

                {
                    std::unique_lock<mutex> lock(worker.mu);
                    worker.cv.wait(lock, [&]() {
                        return worker.has_work || worker.shutdown;
                    });

                    if (worker.shutdown) break;

                    msg = worker.pending_msg;
                    worker.has_work = false;
                }

                // send message to subscriber
                bool success = NetworkUtils::send_message(worker.socket, *msg);
                if (success) {
                    Message ack;
                    success = NetworkUtils::recv_message(worker.socket, ack);
                }
                worker.ack_received = success;

                // signal completion
                {
                    lock_guard<mutex> lock(sub_ack_mu_);
                    if (success) sub_ack_count_++;
                    sub_ack_cv_.notify_one();
                }
            }
            close(worker.socket);
       }

        // original member vars -----------------------------------------------
        unordered_map<NodeId, deque<Message>> proxy_messages_;

        // threading safety
        mutex mu_;
        atomic<bool> running_;
        thread failure_detector_thread_;

        // connection pool optimization
        ConnectionPool connection_pool_;

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
            return config_.subscribers.size();
        }
    };
}}
