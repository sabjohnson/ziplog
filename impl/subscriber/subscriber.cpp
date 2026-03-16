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
        cout << "Subscriber " << id() << " shutdown() called" << endl;
        connection_pool_.close_all();
        log_.print_expanded();
        log_.print_pending();
        log_.print_out_of_order();
        cout << "Subscriber " << id() << " shutdown() complete" << endl;
    }

    void Subscriber::shutdown()
    {
        BaseNode::shutdown();
    }

    void Subscriber::handle_connection(int server_sock)
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

} // namespace ziplog::impl