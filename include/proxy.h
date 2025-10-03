#pragma once
#include "base_node.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

   class Proxy : public BaseNode {
       private:
            // slot allocation
           size_t batch_size_;
           vector<Timestamp> batch_times_;
           vector<Command> batch_values_;

           void epoch_timer();

           void handle_connection(int client_socket) override;
           bool handle_append(const Command& data);
           bool replicate_on_quorum(Message& msg);
          
       public:
           Proxy(NodeId proxy_id, const ZiplogConfig& config);
           void shutdown() override;
           ~Proxy();
   };
}}