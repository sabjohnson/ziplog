#pragma once
#include "config.h"
#include "network_utils.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

   // Note: this class has mixed functionality with clients/proxies from the paper
   class Proxy {
       private:
            // base node
           ZiplogConfig config_;
           ShardId shard_id_;
           NodeId id_;
           string ip_address_;
           int port_;
           atomic<bool> is_running_;

            // slot allocation
           int batch_size_;
           vector<Timestamp> batch_times_;
           vector<Command> batch_values_;

           // threading
           mutex mu_;
           thread epoch_thread_;

           void epoch_timer();
          
       public:
           Proxy(NodeId proxy_id, const ZiplogConfig& config);
           bool append(const Command& data);
           void shutdown();
           ~Proxy();
   };
}}