#pragma once
#include "base_node.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

   // Note: this class has mixed functionality with clients/proxies from the paper
   class Proxy : public BaseNode {
       private:
            // slot allocation
           size_t batch_size_;
           vector<Timestamp> batch_times_;
           vector<Command> batch_values_;

           // threading
           mutex mu_;
           thread epoch_thread_;

           void start_epochs() {
                epoch_thread_ = thread(&Proxy::epoch_timer, this);  // inits epoch_startup_
           }

           void epoch_timer();
          
       public:
           Proxy(NodeId proxy_id, const ZiplogConfig& config);
           void shutdown() override;
           ~Proxy();

           bool append(const Command& data);
   };
}}