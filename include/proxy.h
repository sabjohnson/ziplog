#pragma once
#include "base_node.h"
#include <deque>

using namespace ziplog::api;
using std::deque;

namespace ziplog {
namespace impl {

   class Proxy : public BaseNode {
       private:
           // slot allocation
           SequenceNumber request_count_;           // how many requests a proxy received during this epoch
           deque<SequenceNumber> estimate_history_; // dequeue of request_count for the last couple epochs

           // sequence numbers and interval scheduling
           size_t cur_sequences_size_;              // number of sequence numbers allocated
           deque<SequenceNumber> sequences_;      // sequence numbers allocated
           uint32_t BATCH_INTERVAL;

           // client request information (commands and sockets)
           deque<Command> batch_values_;          // the actual command
           deque<int> client_sockets_;              // so you can respond after sending batch

           // epoch tracking
           Timestamp epoch_startup_;
           Timestamp next_send_;

           // threading
           mutex mu_;
           atomic<bool> epoch_running_;
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
           Proxy(NodeId proxy_id, const ZiplogConfig& config);
           void shutdown() override;
           ~Proxy();
   };
}}