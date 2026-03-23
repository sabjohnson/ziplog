#include "client.h"
#include <arpa/inet.h>

using namespace ziplog::api;

namespace ziplog
{
    namespace impl
    {

        Client::Client(Address proxy)
        {
            proxy_ = proxy;
        }

        Client::~Client()
        {
            connection_pool_.close_all();
        }

        bool Client::append(const Command &payload)
        {
            int sock = connection_pool_.get_connection(proxy_);
            if (sock >= 0)
            {
                Timestamp send_time = now();
                cout << "[client] sending request at " << std::to_string(send_time) << std::endl;

                string ts_prefix = std::to_string(send_time) + "|";
                Command data(ts_prefix.begin(), ts_prefix.end());
                data.insert(data.end(), payload.begin(), payload.end());

                Message req;
                req.type = APPEND;
                req.data = data;

                if (!NetworkUtils::send_message(sock, req))
                {
                    connection_pool_.close_connection(proxy_);
                    cout << "client::append() failed to send message request to proxy" << endl;
                    return false;
                }

                Message resp;
                if (!NetworkUtils::recv_message(sock, resp))
                {
                    connection_pool_.close_connection(proxy_);
                    cout << "client::append() failed to recv proxy response" << endl;
                    return false;
                }

                return resp.type == SUCCESS;
            }
            cout << "client::append() failed to connect to proxy" << endl;
            return false;
        }

        bool Client::bulk_append(const vector<Command> &commands)
        {
            size_t success = 0;
            for (const auto &command : commands)
            {
                if (append(command))
                    success++;
            }
            cout << "Sent " << success << "/" << commands.size() << " commands" << endl;
            return success == commands.size();
        }

        void Client::update_proxy(Address new_proxy)
        {
            connection_pool_.close_all();
            proxy_ = new_proxy;
        }
    }
}