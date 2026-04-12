#include "server/server.h"

using namespace ziplog::api;

namespace ziplog::impl
{

    Server::Server(const ServerConfig &cfg)
        : BaseNode<ServerConfig>(cfg), liveness_(EpochDurationUnit(cfg.epoch_duration),
                                                 EpochDurationUnit(cfg.epoch_duration * 5), // lag = 5 epochs rn bc microseconds
                                                 cfg.proxies.size(),
                                                 cfg.zipper,
                                                 connection_pool_),
          broadcaster_(connection_pool_), freeze_handler_(id(), cfg.zipper, cfg.other_servers,
                                                          connection_pool_, store_, liveness_, broadcaster_,
                                                          [this]()
                                                          { return shard(); })
    {
        broadcaster_.start(cfg.subscribers);
        start_listening();
        // liveness_.start(id());
    }

    Server::~Server()
    {
        ZLOG("Server " << id() << " shutdown() called");
        broadcaster_.shutdown();
        // liveness_.stop();
        BaseNode::shutdown();
        connection_pool_.close_all();
        ZLOG("Server " << id() << " shutdown() complete");
    }

    void Server::shutdown()
    {
        BaseNode::shutdown();
    }

    void Server::handle_connection(int proxy_socket)
    {
        while (running())
        {
            Message msg;
            if (!NetworkUtils::recv_message(proxy_socket, msg))
                break;

            if (msg.type == APPEND || msg.type == SKIP)
            {
                auto start = high_resolution_clock::now();
                if (msg.type == APPEND)
                {
                    // cout << "[server " << id() << "] got batch at " << std::to_string(now()) << std::endl;
                }

                Message ack;
                ack.type = ACK;
                ack.shard_id = shard();
                ack.sender_id = id();
                ack.set_sequence_number(msg.get_sequence_number());
                NetworkUtils::send_message(proxy_socket, ack);
                auto end = high_resolution_clock::now();

                auto dur = duration_cast<microseconds>(end - start);
                cout << "Server ack latency: " << dur.count() << " us\n";

                if (!liveness_.is_blocked(msg.sender_id))
                {
                    store_.store(msg.sender_id, msg);
                    liveness_.remove_timeout(msg.sender_id, msg.get_sequence_number());
                    broadcaster_.broadcast(msg);
                }
            }
            else if (msg.type == ZIP_RESPONSE)
            {
                liveness_.update_timeouts(msg.sender_id, msg.ordering_values);

                Message ack;
                ack.type = ACK;
                ack.shard_id = shard();
                ack.sender_id = id();
                NetworkUtils::send_message(proxy_socket, ack);
            }
            else if (msg.type == FREEZE)
                freeze_handler_.handle_freeze(msg, true);
            else if (msg.type == TRANSFER_REQUEST)
                freeze_handler_.handle_transfer_request(proxy_socket, msg);
            else if (msg.type == FREEZE_COMPLETE)
                liveness_.block_proxy(msg.get_failed_proxy());
            else if (msg.type == INCLUDE_PROXY)
                introduce_proxy(msg);
            else if (msg.type == INCLUDE_SUBSCRIBER)
                introduce_subscriber(proxy_socket, msg);
        }
        close(proxy_socket);
    }

    void Server::introduce_proxy(const Message &msg)
    {
        string addr_info(msg.data.begin(), msg.data.end());
        size_t colon = addr_info.find(':');
        string ip = addr_info.substr(0, colon);
        int port = stoi(addr_info.substr(colon + 1));

        NodeId proxy_id = msg.sender_id;
        if (!config_.isValidProxy(proxy_id))
        {
            config_.proxies.push_back({ip, port});
        }
        else
        {
            liveness_.unblock_proxy(proxy_id);
        }

        Message ack;
        ack.type = ACK;
        ack.shard_id = shard();
        ack.sender_id = id();
        NetworkUtils::send_message(/* caller holds socket */ 0, ack); // ack sent by caller
    }

    void Server::introduce_subscriber(int socket, const Message &msg)
    {
        string addr_info(msg.data.begin(), msg.data.end());
        size_t colon = addr_info.find(':');
        string ip = addr_info.substr(0, colon);
        int port = stoi(addr_info.substr(colon + 1));
        Address subscriber{ip, port};

        config_.subscribers.push_back(subscriber);
        auto snap = store_.snapshot();

        Message ack;
        ack.type = ACK;
        ack.shard_id = shard();
        ack.sender_id = id();
        NetworkUtils::send_message(socket, ack);

        // replay stored messages to new subscriber
        int sock = connection_pool_.get_connection(subscriber);
        if (sock < 0)
            return;

        for (const auto &[proxy_id, messages] : snap)
        {
            if (liveness_.is_blocked(proxy_id))
                continue;
            for (const Message &stored : messages)
            {
                Message fwd = stored;
                fwd.sender_id = id();
                Message resp;
                NetworkUtils::send_message(sock, fwd);
                NetworkUtils::recv_message(sock, resp);
            }
        }

        broadcaster_.add_subscriber(config_.subscribers.size() - 1, subscriber);
    }

} // namespace ziplog::impl