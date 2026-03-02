#pragma once
#include "base_node.h"
#include "connection_pool.h"
#include <condition_variable>
#include <deque>
#include <queue>

using namespace ziplog::api;
using std::condition_variable;
using std::deque;
using std::queue;

namespace ziplog {
namespace impl {

    // Storage servers in the paper
    class Server : public BaseNode<ServerConfig> {
    private:
       struct ReplicationState {
            mutex mu;
            condition_variable cv;
            atomic<int> ack_count{0};
            SequenceNumber seq;
       };

       struct WorkItem {
            Message msg;
            std::shared_ptr<ReplicationState> state;
       };

        struct SubscriberWorker {
           thread worker_thread;
           int socket;
           queue<WorkItem> pending_queue;
           mutex queue_mu;
           condition_variable cv;
           atomic<bool> shutdown{false};
       };

       std::unordered_map<int, std::unique_ptr<SubscriberWorker>> subscriber_workers_;
       mutex subscriber_workers_mu_;

       void subscriber_worker_loop(int subscriber_idx) {
            Address subscriber = config_.subscribers[subscriber_idx];
            SubscriberWorker& worker = *subscriber_workers_[subscriber_idx];

            // create persistent connection
            worker.socket = NetworkUtils::connect_to_address_persistent(subscriber.ip, subscriber.port);
            if (worker.socket < 0) {
                worker.shutdown = true;
            }

            while (!worker.shutdown) {
                WorkItem item;

                // wait until the queue has a message or the worker is being shutdown
                {
                    std::unique_lock<mutex> lock(worker.queue_mu);
                    worker.cv.wait(lock, [&]() {
                        return !worker.pending_queue.empty() || worker.shutdown;
                    });

                    if (worker.shutdown) break;

                    // pop from queue
                    item = worker.pending_queue.front();
                    worker.pending_queue.pop();
                }

                // attempt to send message to subscriber and wait for ack (blocking)
                cout << "server sending a message" << endl;
                bool success = NetworkUtils::send_message(worker.socket, item.msg);
                if (success) {
                    Message ack;
                    success = NetworkUtils::recv_message(worker.socket, ack);
                }

                if (!success) {
                    worker.shutdown = true;
                    continue;
                }

                // signal completion
                if (success && item.state) {
                    lock_guard<mutex> lock(item.state->mu);
                    item.state->ack_count++;
                    item.state->cv.notify_one();
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
