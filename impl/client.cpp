#include "client.h"

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Client::Client(Address proxy) {
        proxy_ = proxy;
    }

    bool Client::append(const Command& data) {
        int sock = connection_pool_.get_connection(proxy_);
        if (sock >= 0) {
            Message req;
            req.type = APPEND;
            req.data = data;

            if (!NetworkUtils::send_message(sock, req)) return false;

            Message resp;
            if (!NetworkUtils::recv_message(sock, resp)) return false;

            return resp.type == SUCCESS;
        }
        return false;
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