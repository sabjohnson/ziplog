#pragma once
#include "base_node.h"
#include "connection_pool.h"
#include "circular_buffer.h"
#include <deque>

using namespace ziplog::api;
using std::deque;

namespace ziplog {
namespace impl {

   class Proxy : public BaseNode<ProxyConfig> {
       private:
           struct ServerWorker {
//               thread worker_thread;
               std::unique_ptr<thread> worker_thread;
               int socket;
               mutex mu;
               condition_variable cv;

               optional<Message> pending_msg;
               bool has_work = false;
               bool shutdown = false;

               // reslut signaling
               atomic<bool> ack_received{false};
           };

           std::unordered_map<int, std::unique_ptr<ServerWorker>> server_workers_;
           mutex server_workers_mu_;

           // coordination for f+1 ACKs
           mutex ack_mu_;
           condition_variable ack_cv_;
           atomic<int> ack_count_{0};

           void server_worker_loop(int server_idx) {
                Address server = config_.servers[server_idx];
                ServerWorker& worker = *server_workers_[server_idx];

                // create persistent connection
                worker.socket = NetworkUtils::connect_to_address_persistent(server.ip, server.port);
                if (worker.socket < 0) {
                    return;
                }

                while (true) {
                    optional<Message> msg;

                    // wait for signal (has work)
                    {
                        std::unique_lock<mutex> lock(worker.mu);
                        worker.cv.wait(lock, [&]() {
                            return worker.has_work || worker.shutdown;
                        });

                        if (worker.shutdown) break;

                        msg = worker.pending_msg;
                        worker.has_work = false;
                    }

                    // send message to replica
                    bool success = NetworkUtils::send_message(worker.socket, *msg);
                    if (success) {
                        Message ack;
                        success = NetworkUtils::recv_message(worker.socket, ack);
                    }
                    worker.ack_received = success;

                    // signal to epoch thread that we got an ack
                    {
                        lock_guard<mutex> lock(ack_mu_);
                        if (success) ack_count_++;
                        ack_cv_.notify_one();
                    }
                }
                close(worker.socket);
           }

           // originla vars ------------------------------------------------------
           atomic<bool> registered_ = true;

           // slot allocation
           SequenceNumber request_count_ = 0;           // how many requests a proxy received during this epoch
           deque<SequenceNumber> estimate_history_; // dequeue of request_count for the last couple epochs

           // sequence numbers and interval scheduling
           deque<SequenceNumber> sequences_;        // sequence numbers allocated
           deque<Timestamp> timeouts_;              // time outs for the corresponding sequnec number
           Timestamp next_send_ = 0;                    // timestamp of next timeouts for sending a batch (is 0 if no slots allocated)

           // client request information (commands and sockets)
           std::unordered_map<int, CircularBuffer<PendingRequest>> client_buffers_;  // socket → buffer
           std::mutex buffers_mu_;

           // Track which client sockets are pending responses for a given sequence number
          std::unordered_map<SequenceNumber, std::vector<int>> seq_to_clients_;
          std::mutex clients_mu_;  // Protects seq_to_clients_


           //deque<Command> batch_values_;            // the actual command
           //deque<int> client_sockets_;              // so you can respond after sending batch

           // epoch tracking
           Timestamp epoch_startup_;

           // threading
           mutex epoch_mu_;
           atomic<bool> epoch_running_;
           thread epoch_thread_;

           // connection pool optimization (to just zipper)
           ConnectionPool zipper_connection_pool_;

           void start_epochs() {
                epoch_thread_ = thread(&Proxy::epoch_timer, this);
           }

           void epoch_timer();
           void update_slot_estimate();
           void update_next_send();
           void send_out_batch();

           void handle_connection(int client_socket) override;
           void handle_zip_response(Message& msg);
           void handle_append(int client_socket, const Command& data);
           bool replicate_on_quorum(Message& msg);
          
       public:
           Proxy(const ProxyConfig& config);
           Proxy(const ProxyConfig& config, bool registered);
           void shutdown() override;
           ~Proxy();
           void attempt_join(bool is_new);

           size_t num_servers() const {
               return config_.servers.size();
           }
   };
}}