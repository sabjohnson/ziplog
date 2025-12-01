#pragma once
#include "base_node.h"
#include <deque>
#include <condition_variable>

using namespace ziplog::api;
using std::deque;

namespace ziplog {
namespace impl {

   class Proxy : public BaseNode<ProxyConfig> {
       private:
           atomic<bool> registered_ = true;

           // slot allocation
           SequenceNumber request_count_ = 0;           // how many requests a proxy received during this epoch
           deque<SequenceNumber> estimate_history_; // dequeue of request_count for the last couple epochs

           // sequence numbers and interval scheduling
           deque<SequenceNumber> sequences_;        // sequence numbers allocated
           deque<Timestamp> timeouts_;              // time outs for the corresponding sequnec number
           Timestamp next_send_ = 0;                    // timestamp of next timeouts for sending a batch (is 0 if no slots allocated)

           // client request information (commands and sockets)
           deque<Command> batch_values_;            // the actual command
           deque<int> client_sockets_;              // so you can respond after sending batch

           // epoch tracking
           Timestamp epoch_startup_ = 0;

           // threading
           mutex mu_;
           mutex epoch_cv_mutex_;
           std::condition_variable epoch_cv_;
           atomic<bool> epoch_running_ = false;
           thread epoch_thread_;

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