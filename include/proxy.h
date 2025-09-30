#pragma once
#include "config.h"
#include "network_utils.h"
#include <atomic>

using namespace ziplog::api;

namespace ziplog {
namespace impl {

   // Note: this class has mixed functionality with clients/proxies from the paper
   class Proxy {
       private:
           ziplogConfig config;
           int shard_id;
           int id;
           string ipAddress;
           int port;
           std::atomic<bool> isRunning;
          
       public:
           Proxy(int proxy_id, const ziplogConfig& config);
           bool append(const string& data);
           void shutdown();
   };
}}