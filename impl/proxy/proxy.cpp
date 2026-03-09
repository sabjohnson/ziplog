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
                                                                                                                      cfg.epoch_duration_ms,
                                                                                                                      [this]()
                                                                                                                      { update_slot_estimate(); },
                                                                                                                      [this]()
                                                                                                                      { return slot_scheduler_.next_send() != 0 && now_ms() >= slot_scheduler_.next_send(); },
                                                                                                                      [this]()
                                                                                                                      { send_out_batch(); })
    {
        replicator_.start();
        start_listening();
        epoch_timer_.start();
    }

    Proxy::Proxy(const ProxyConfig &cfg, bool registered)
        : BaseNode<ProxyConfig>(cfg), slot_scheduler_(cfg.max_epoch_history), replicator_(cfg.servers, quorum()), epoch_timer_(
                                                                                                                      cfg.epoch_duration_ms,
                                                                                                                      [this]()
                                                                                                                      { update_slot_estimate(); },
                                                                                                                      [this]()
                                                                                                                      { return slot_scheduler_.next_send() != 0 && now_ms() >= slot_scheduler_.next_send(); },
                                                                                                                      [this]()
                                                                                                                      { send_out_batch(); })
    {
        registered_ = registered;
        replicator_.start();
        attempt_join(true);
        start_listening();
        // epoch_timer_.start() called after INCLUDE_PROXY received
    }

    Proxy::~Proxy()
    {
        cout << "Proxy " << id() << " shutting down" << endl;
        epoch_timer_.stop();
        replicator_.shutdown();
        zipper_pool_.close_all();
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
                if (!registered_)
                {
                    Message resp;
                    resp.type = FAILURE;
                    NetworkUtils::send_message(client_socket, resp);
                    continue;
                }
                cout << "[proxy " << id() << "] recv client req on socket " << client_socket << endl;
                client_buffers_.push(client_socket, req.data);
                slot_scheduler_.record_request();
                cout << "[proxy " << id() << "] buffer size: "
                     << client_buffers_.buffer_size(client_socket) << endl;
            }
            else if (req.type == ZIP_RESPONSE)
                handle_zip_response(req);
            else if (req.type == INCLUDE_PROXY)
            {
                if (!registered_)
                {
                    registered_ = true;
                    epoch_timer_.start();
                    cout << "[proxy " << id() << "] joined the system" << endl;
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
        cout << "[proxy " << id() << "] closed socket " << client_socket << endl;
    }

    void Proxy::handle_zip_response(const Message &msg)
    {
        if (msg.shard_id != shard())
            return;
        cout << "[proxy " << id() << "] got " << msg.get_num_requests() << " slots from zipper" << endl;
        slot_scheduler_.load_slots(msg.ordering_values);
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
        SequenceNumber estimate = slot_scheduler_.compute_estimate();
        cout << "[proxy " << id() << "] sending estimate " << estimate << " to zipper" << endl;

        Message req;
        req.type = ZIP_REQUEST;
        req.shard_id = shard();
        req.sender_id = id();
        req.set_num_requests(estimate);

        int sock = zipper_pool_.get_connection(config_.zipper);
        if (sock < 0)
        {
            cout << "[proxy " << id() << "] could not reach zipper" << endl;
            return;
        }
        if (!NetworkUtils::send_message(sock, req))
            cout << "[proxy " << id() << "] failed to send estimate to zipper" << endl;
    }

    void Proxy::send_out_batch()
    {
        SequenceNumber seq;
        Timestamp send_time;
        if (!slot_scheduler_.pop_next_slot(seq, send_time))
        {
            cout << "[proxy " << id() << "] no slots available" << endl;
            return;
        }
        cout << "[proxy " << id() << "] send_oyt_batch() called" << endl;
        Message msg;
        msg.shard_id = shard();
        msg.sender_id = id();
        msg.set_sequence_number(seq);

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
        }

        bool success = replicator_.replicate(msg);

        Message resp;
        resp.type = success ? SUCCESS : FAILURE;
        cout << "[proxy " << id() << "] replication "
             << (success ? "succeeded" : "failed")
             << " for seq " << seq << endl;

        for (int client : participating)
        {
            NetworkUtils::send_message(client, resp);
        }
    }
} // namespace ziplog::impl