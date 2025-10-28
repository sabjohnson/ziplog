#include "client.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Client::Client(const ZiplogConfig& cfg, NodeId proxy_id)
        : config_(cfg)
        , proxy_id_(proxy_id)
    {
        validate_node_id(proxy_id, cfg.num_proxies(), "Proxy");
    }

    bool Client::append(const Command& data) {
        auto [proxy_ip, proxy_port] = config_.proxies[proxy_id_];

        Message msg;
        msg.type = APPEND;
        msg.data = data;
        cout << "sending smthn" << endl;
        return NetworkUtils::send_message_to_address(proxy_ip, proxy_port, msg, config_.timeout_ms, config_.max_retries);
    }

    bool Client::bulk_append(const vector<Command>& commands) {
        size_t success = 0;
        for (const auto& command : commands) {
            if (append(command)) success++;
        }
        cout << "Sent " << success << "/" << commands.size() << " commands" << endl;
        return success == commands.size();
    }
}}