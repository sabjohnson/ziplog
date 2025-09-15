#pragma once
#include "config.h"
#include "network_utils.h"
#include <stdexcept>

using namespace ziplog::api;

namespace ziplog {
namespace impl {


   class Client {
       // config info
       int id;
       ziplogConfig config;
       // tcp info
       string ipAddress;
       int port;
       bool isRunning;
       int operation_number;
      
       public:
           Client(int client_id, const ziplogConfig& config);
           bool append(const string& data);
           void shutdown();
   };
}}