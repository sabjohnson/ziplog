#pragma once
#include "config.h"
#include "network_utils.h"
#include <atomic>

using namespace ziplog::api;

namespace ziplog {
namespace impl {


   class Client {
       private:
           int id;
           ziplogConfig config;
           string ipAddress;
           int port;
           std::atomic<bool> isRunning;
           int operation_number;
          
       public:
           Client(int client_id, const ziplogConfig& config);
           bool append(const string& data);
           void shutdown();
   };
}}