#pragma once
#include "base_node.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    // Storage servers in the paper
    class Server : public BaseNode {
    private:
        void handle_connection(int proxy_socket) override;
        void broadcast_to_subscribers(const Message& msg);

    public:
        Server(NodeId server_id, const ZiplogConfig& cfg);
        void shutdown() override;
        ~Server();
    };
}}
