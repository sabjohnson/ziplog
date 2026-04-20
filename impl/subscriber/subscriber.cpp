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

    void Subscriber::handle_connection(int server_sock)
    {
        NetworkUtils::ReadBuffer rb;

        while (running())
        {
            Message msg;
            if (!NetworkUtils::recv_message_buffered(server_sock, rb, msg))
                break;

            auto t0 = high_resolution_clock::now();

            std::vector<std::pair<SequenceNumber, int64_t>> latencies;
            if (msg.type == APPEND)
            {
                latencies = log_.observe(msg.sender_id, msg.get_sequence_number(), msg.data, quorum());
            }
            else if (msg.type == SKIP)
            {
                latencies = log_.observe(msg.sender_id, msg.get_sequence_number(), Command(), quorum());
            }

            auto t1 = high_resolution_clock::now();

            Message ack;
            ack.type = ACK;
            ack.sender_id = id();
            ack.seq_or_count = msg.seq_or_count;
            if (!NetworkUtils::send_message(server_sock, ack))
                break;

            auto end = high_resolution_clock::now();
            auto observe_dur = duration_cast<EpochDurationUnit>(t1 - t0);
            auto ack_dur = duration_cast<EpochDurationUnit>(end - t1);

            // print after ack — completely off critical path
            for (auto &[seq, lat] : latencies)
            {
                cout << "[latency] seq=" << seq << " latency=" << lat
                     << " " << EPOCH_DURATION_UNIT_STR << "\n";
            }
            cout << "Subscriber log observe latency: " << observe_dur.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
            cout << "Subscriber ack latency: " << ack_dur.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
        }
        close(server_sock);
    }

} // namespace ziplog::impl