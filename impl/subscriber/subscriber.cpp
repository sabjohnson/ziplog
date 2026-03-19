#include "subscriber.h"

using namespace ziplog::api;

namespace ziplog::impl
{

    Subscriber::Subscriber(const SubscriberConfig &cfg)
        : BaseNode<SubscriberConfig>(cfg)
    {
        start_listening();
    }

    Subscriber::Subscriber(const SubscriberConfig &cfg, bool registered)
        : BaseNode<SubscriberConfig>(cfg)
    {
        start_listening();

        Message req;
        req.type = REGISTER_SUBSCRIBER;
        req.shard_id = shard();
        req.sender_id = id();
        string addr = address().ip + ":" + std::to_string(address().port);
        req.data = Command(addr.begin(), addr.end());

        int sock = connection_pool_.get_connection(config_.zipper);
        if (sock >= 0)
            NetworkUtils::send_message(sock, req);
    }

    Subscriber::~Subscriber()
    {
        ZLOG("Subscriber " << id() << " shutdown() called");
        BaseNode::shutdown();
        connection_pool_.close_all();
        log_.print_expanded();
        log_.print_pending();
        log_.print_out_of_order();
        ZLOG("Subscriber " << id() << " shutdown() complete");
    }

    void Subscriber::shutdown()
    {
        BaseNode::shutdown();
    }
    /*
        void Subscriber::handle_connection_og(int server_sock)
        {
            while (running())
            {
                Message msg;
                if (!NetworkUtils::recv_message(server_sock, msg))
                    break;

                if (msg.type == APPEND || msg.type == SKIP)
                    log_.observe(msg.sender_id, msg.get_sequence_number(), msg.data, quorum());

                Message ack;
                ack.type = ACK;
                ack.sender_id = id();
                ack.seq_or_count = msg.seq_or_count;

                if (!NetworkUtils::send_message(server_sock, ack))
                    break;
            }
            close(server_sock);
        }
    */
    void Subscriber::handle_connection(int server_sock)
    {
        while (running())
        {
            Message msg;
            if (!NetworkUtils::recv_message(server_sock, msg))
                break;

            if (msg.type == APPEND)
            {
                Timestamp received = now();
                vector<Command> commands = CommandBatch::deserialize(msg.data);

                for (auto &command : commands)
                {
                    Command payload = command;

                    auto it = std::find(command.begin(), command.end(), '|');
                    if (it != command.end())
                    {
                        string ts_str(command.begin(), it);
                        if (!ts_str.empty() && std::all_of(ts_str.begin(), ts_str.end(), [](unsigned char c)
                                                           { return std::isdigit(c); }))
                        {
                            Timestamp sent = std::stoull(ts_str); // stoull() converts string to unsigned long long
                            payload = Command(it + 1, command.end());
                            int64_t latency = static_cast<int64_t>(received) - static_cast<int64_t>(sent);
                            cout << "[latency] seq=" << msg.get_sequence_number()
                                 << " latency=" << latency << " " << EPOCH_DURATION_UNIT_STR
                                 << " payload=" << command_to_string(payload) << endl;
                        }
                        else
                        {
                            cout << "[latency] bad ts_str: " << "-" << ts_str << "-" << endl;
                        }
                    }
                }
                log_.observe(msg.sender_id, msg.get_sequence_number(), msg.data, quorum());
            }
            else if (msg.type == SKIP)
            {
                log_.observe(msg.sender_id, msg.get_sequence_number(), Command(), quorum());
            }

            Message ack;
            ack.type = ACK;
            ack.sender_id = id();
            ack.seq_or_count = msg.seq_or_count;
            if (!NetworkUtils::send_message(server_sock, ack))
                break;
        }
        close(server_sock);
    }

} // namespace ziplog::impl