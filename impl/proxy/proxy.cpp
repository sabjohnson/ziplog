#include "proxy.h"
#include <math.h>

using namespace ziplog::api;

namespace ziplog::impl
{

    /* -----------------------------------------------------------------------------------------------------------------------
        Construction/Deconstruction
    ----------------------------------------------------------------------------------------------------------------------- */

    Proxy::Proxy(const ProxyConfig &cfg)
        : BaseNode<ProxyConfig>(cfg), slot_scheduler_(cfg.max_epoch_history), replicator_(cfg.servers, quorum()), epoch_timer_(
                                                                                                                      EpochDurationUnit(cfg.epoch_duration),
                                                                                                                      [this]()
                                                                                                                      { update_slot_estimate(); },
                                                                                                                      [this](Timestamp &ts, SequenceNumber &seq)
                                                                                                                      { return slot_scheduler_.pop_next_slot(ts, seq); },
                                                                                                                      [this](SequenceNumber seq)
                                                                                                                      { send_out_batch(seq); })
    {
        replicator_.start();
        start_listening();
        epoch_timer_.start();
    }

    Proxy::Proxy(const ProxyConfig &cfg, bool registered)
        : BaseNode<ProxyConfig>(cfg), slot_scheduler_(cfg.max_epoch_history), replicator_(cfg.servers, quorum()), epoch_timer_(
                                                                                                                      EpochDurationUnit(cfg.epoch_duration),
                                                                                                                      [this]()
                                                                                                                      { update_slot_estimate(); },
                                                                                                                      [this](Timestamp &ts, SequenceNumber &seq)
                                                                                                                      { return slot_scheduler_.pop_next_slot(ts, seq); },
                                                                                                                      [this](SequenceNumber seq)
                                                                                                                      { send_out_batch(seq); })
    {
        registered_ = registered;
        replicator_.start();
        attempt_join(true);
        start_listening();
        // epoch_timer_.start() called after INCLUDE_PROXY received
    }

    Proxy::~Proxy()
    {
        ZLOG("Proxy " << id() << " shutdown() called");
        epoch_timer_.stop();
        BaseNode::shutdown();
        replicator_.shutdown();
        zipper_pool_.close_all();
        ZLOG("Proxy " << id() << " shutdown() complete");
    }

    /* -----------------------------------------------------------------------------------------------------------------------
        Message Handling
    ----------------------------------------------------------------------------------------------------------------------- */

    void Proxy::handle_connection(int client_socket)
    {
        while (running())
        {
            Message req;
            if (!NetworkUtils::recv_message(client_socket, req))
                break;

            if (req.type == APPEND)
            {
                auto start = high_resolution_clock::now();
                if (!registered_)
                {
                    Message resp;
                    resp.type = FAILURE;
                    NetworkUtils::send_message(client_socket, resp);
                    continue;
                }
                ZLOG("[proxy " << id() << "] recv client req on socket " << client_socket);

                client_buffers_.push(client_socket, req.data);
                auto client_buffers_end = high_resolution_clock::now();

                slot_scheduler_.record_request();
                auto slot_sched_end = high_resolution_clock::now();

                ZLOG("[proxy " << id() << "] buffer size: "
                               << client_buffers_.buffer_size(client_socket));

                auto dur1 = duration_cast<EpochDurationUnit>(client_buffers_end - start);
                auto dur2 = duration_cast<EpochDurationUnit>(slot_sched_end - start);

                cout << "Proxy client buffer - push: " << dur1.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
                cout << "Proxy slot scheduler - record request: " << dur2.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
            }
            else if (req.type == ZIP_RESPONSE)
                handle_zip_response(req);
            else if (req.type == INCLUDE_PROXY)
            {
                if (!registered_)
                {
                    registered_ = true;
                    epoch_timer_.start();
                    ZLOG("[proxy " << id() << "] joined the system");
                }
            }
            else if (req.type == FREEZE)
            {
                registered_ = false;
                epoch_timer_.pause();
                attempt_join(false);
            }
        }

        close(client_socket);
        client_buffers_.remove(client_socket);
        ZLOG("[proxy " << id() << "] closed socket " << client_socket);
    }

    void Proxy::handle_zip_response(const Message &msg)
    {
        if (msg.shard_id != shard())
            return;

        ZLOG("[proxy " << id() << "] got " << msg.get_num_requests() << " slots from zipper");
        auto start = high_resolution_clock::now();

        // cout << "[proxy " << id() << "] got slots from zipper at " << std::to_string(now()) << endl;
        slot_scheduler_.load_slots(msg.ordering_values);

        ZLOG("[proxy " << id() << "] load slots returned");
        auto end = high_resolution_clock::now();

        auto dur = duration_cast<EpochDurationUnit>(end - start);
        cout << "Proxy load zipper slots: " << dur.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
    }

    void Proxy::attempt_join(bool is_new)
    {
        Message req;
        req.type = is_new ? REGISTER_PROXY : REJOIN_PROXY;
        req.shard_id = shard();
        if (!is_new)
            req.sender_id = id();

        string addr = address().ip + ":" + std::to_string(address().port);
        req.data = Command(addr.begin(), addr.end());

        int sock = zipper_pool_.get_connection(config_.zipper);
        if (sock < 0)
            return;
        NetworkUtils::send_message(sock, req);
    }

    /* -----------------------------------------------------------------------------------------------------------------------
        Epoch Callbacks
    ----------------------------------------------------------------------------------------------------------------------- */

    void Proxy::update_slot_estimate()
    {
        auto start = high_resolution_clock::now();

        SequenceNumber estimate = slot_scheduler_.compute_estimate();

        Message req;
        req.type = ZIP_REQUEST;
        req.shard_id = shard();
        req.sender_id = id();
        req.set_num_requests(estimate);

        if (estimate)
        {
            ZLOG("[proxy " << id() << "] sending estimate " << estimate << " to zipper");
        }

        int sock = zipper_pool_.get_connection(config_.zipper);
        if (sock < 0)
        {
            ZLOG("[proxy " << id() << "] could not reach zipper");
            return;
        }
        if (!NetworkUtils::send_message(sock, req))
        {
            ZLOG("[proxy " << id() << "] failed to send estimate to zipper");
        }
        auto end = high_resolution_clock::now();
        auto dur = duration_cast<EpochDurationUnit>(end - start);

        if (estimate)
        {
            cout << "Proxy update slot estimate(): " << dur.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
        }
    }

    void Proxy::send_out_batch(SequenceNumber seq)
    {
        auto start = high_resolution_clock::now();
        ZLOG("[proxy " << id() << "] send_out_batch() called");
        Message msg;
        msg.shard_id = shard();
        msg.sender_id = id();

        auto [batch, participating] = client_buffers_.drain_batch();

        if (participating.empty())
        {
            msg.type = SKIP;
            msg.data = Command();
        }
        else
        {
            msg.type = APPEND;
            msg.data = batch.serialize();
            Timestamp send_time = now();
            // cout << "[proxy " << id() << "] sending batch out at " << std::to_string(send_time) << std::endl;
        }

        msg.set_sequence_number(seq);
        bool success = replicator_.replicate(msg);

        Message resp;
        resp.type = success ? SUCCESS : FAILURE;
        ZLOG("[proxy " << id() << "] replication "
                       << (success ? "succeeded" : "failed")
                       << " for seq " << seq);

        for (int client : participating)
        {
            NetworkUtils::send_message(client, resp);
        }
        auto end = high_resolution_clock::now();
        auto dur = duration_cast<EpochDurationUnit>(end - start);
        cout << "Proxy send out batch(): " << dur.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
    }
} // namespace ziplog::impl
