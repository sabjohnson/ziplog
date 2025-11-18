#include "proxy.h"
#include <math.h>
#include <future>

using namespace ziplog::api;
using std::future;

namespace ziplog {
namespace impl {


    /* -----------------------------------------------------------------------------------------------------------------------
        Construction/Deconstruction
     ----------------------------------------------------------------------------------------------------------------------- */

    Proxy::Proxy(NodeId proxy_id, const ZiplogConfig &cfg)
        : BaseNode(proxy_id, cfg, cfg.proxies[proxy_id].first, cfg.proxies[proxy_id].second)
    {
        // validate node id
        validate_node_id(proxy_id, cfg.num_proxies(), "Proxy");

        // set value of members (we already know ip addr is in our valid range based on parsed config)
        request_count_ = 0;
        next_send_ = 0;

        start_listening();
        start_epochs();
    }

    Proxy::Proxy(NodeId proxy_id, const ZiplogConfig &cfg, bool registered)
        : BaseNode(proxy_id, cfg, cfg.proxies[proxy_id].first, cfg.proxies[proxy_id].second)
    {
        if (!registered) {
            registered_ = registered;
        }

        attempt_join(true);
        start_listening();
    }

    void Proxy::shutdown() {
        BaseNode::shutdown();
    }

    Proxy::~Proxy() {
        cout << "Proxy " << id() << " shutting down" << endl;
        epoch_running_ = false;
        if (epoch_thread_.joinable()) {
            epoch_thread_.join();
        }

        lock_guard<mutex> lock(mu_);

        Message failure;
        failure.type = FAILURE;
        for (int client : client_sockets_) {
            cout << "[proxy " << id_ << "] sending failure to client on shutdown" << endl;
            NetworkUtils::send_message(client, failure);
            close(client);
        }

        shutdown();

    }

    /* -----------------------------------------------------------------------------------------------------------------------
        Message Passing
     ----------------------------------------------------------------------------------------------------------------------- */

    void Proxy::handle_connection(int client_socket) {
        // read from and respond to valid request
        Message req;
        if (!NetworkUtils::recv_message(client_socket, req)) {
            close(client_socket);
            return;
        }

        cout << "[proxy " << id() << "] ------------------------------------- RECV CLIENT REQ" << endl;

        // build and send response
        Message resp;
        resp.type = ACK;

        if (req.type == APPEND) {
            if (registered_) {
                handle_append(client_socket, req.data);
                return;
            } else {
                cout << "[proxy " << id() << "] not registered yet" << endl;
                Message resp;
                resp.type = FAILURE;
                NetworkUtils::send_message(client_socket, resp);
                close(client_socket);
                return;
            }
        } else if (req.type == ZIP_RESPONSE) {
            handle_zip_response(req);
        }

        else if (req.type == INCLUDE_PROXY) {
            if (!registered_) {
                registered_ = true;
                start_epochs();
                cout << "[proxy " << id() << "] ------------------------------------- joining the system" << endl;
            }
        }

        else if (req.type == FREEZE) {
            registered_ = false;
            attempt_join(false);
        }

        // send response
        NetworkUtils::send_message(client_socket, resp);
        close(client_socket);
    }

    void Proxy::handle_append(int client_socket, const Command& data) {
        // obtain lock
        lock_guard<mutex> lock(mu_);

        // take notes of command
        batch_values_.push_back(data);

        // increment number of requests
        request_count_++;
        //cout << "request count: " << request_count_ << endl;

        // take note of client socket (respond after its replicated during epoch interval)
        client_sockets_.push_back(client_socket);
    }

    // attempt to replicate on f + 1 storage servers
    bool Proxy::replicate_on_quorum(Message& msg) {
        vector<future<bool>> futures;

        for (size_t i = 0; i < config_.num_servers(); i++) {
            auto [server_ip, server_port] = config_.servers[i];

            futures.push_back(std::async(std::launch::async, [=, &msg]() {
                Message resp;
                if (NetworkUtils::send_message_to_address(server_ip, server_port, msg, resp, config_.max_retries)) {
                    return resp.type == ACK;
                }
                return false;
            }));
        }

        size_t successful_sends = 0;
        for (auto& f : futures) {
            if (f.get()) successful_sends++;
        }

        cout << "successful sends = " << successful_sends << " vs " << " quorum = " << config_.quorum() << endl;
        return successful_sends >= config_.quorum();
    }

    void Proxy::handle_zip_response(Message& msg) {
        // obtain lock
        lock_guard<mutex> lock(mu_);

        // validate zip response
        if (msg.shard_id != shard()) return;

        cout << "[proxy "  << id() << "] got " << msg.get_num_requests() << " slots from the zipper" << endl;

        // add new sequence numbers/timeouts to your pool
        for (size_t i = 0; i < msg.ordering_values.size(); i++) {
            if (i % 2 == 0) timeouts_.push_back(msg.ordering_values[i]);
            else sequences_.push_back(msg.ordering_values[i]);
        }

        if (timeouts_.size() > 0) next_send_ = timeouts_.front();
    }

    void Proxy::attempt_join(bool is_new) {
        Message req;
        req.type = is_new ? REGISTER_PROXY : REJOIN_PROXY;
        req.shard_id = shard();
        if (!is_new) req.sender_id = id();

        string addr_info = ip_address_ + ":" + std::to_string(port_);
        req.data = Command(addr_info.begin(), addr_info.end());

        Message resp;
        NetworkUtils::send_message_to_address(
            config_.zipper.first,
            config_.zipper.second,
            req, resp,
            config_.max_retries
        );
    }

    /* -----------------------------------------------------------------------------------------------------------------------
        Epoch handling
     ----------------------------------------------------------------------------------------------------------------------- */

    void Proxy::epoch_timer() {
        cout << "[proxy " << id() << "] calling epoch_timer " << endl;
        epoch_running_ = true;
        epoch_startup_ = now_ms();
        const Timestamp sleep_duration = std::max(static_cast<Timestamp>(1), config_.epoch_duration_ms / 200);

        while (epoch_running_) {
            Timestamp now = now_ms();
            Timestamp elapsed = now - epoch_startup_;

            if (registered_) {
                if (elapsed >= config_.epoch_duration_ms) { // full epoch elapsed
                    // restart state
                    epoch_startup_ = now_ms();
                    update_slot_estimate();
                    //cout << "[ proxy " << id() << "] new epoch for proxy" << endl;

                } else if (next_send_ != 0 && now >= next_send_) { // time to send out next batch
                    //cout << "[ proxy " << id() << "] updating send time for proxy" << endl;
                    std::thread([this]() {
                        send_out_batch();
                    }).detach();
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
        }
    }

    void Proxy::update_slot_estimate() {
        // obtain lock
        mu_.lock();

        // update request history and calculate estimate for the appropriate number of epochs (i.e., min(total epochs, MAX_EPOCH_HISTORY))
        estimate_history_.push_back(request_count_);
        if (id() == 1) cout << "request count = " << request_count_ << endl;
        if (estimate_history_.size() > static_cast<long unsigned int>(config_.max_epoch_history)) {
            estimate_history_.pop_front();
        }
        SequenceNumber avg = 0;
        for (auto est : estimate_history_) {
            avg += est;
        }

        // update your value
        SequenceNumber slot_estimate_ = estimate_history_.empty() ? 0 :
            static_cast<SequenceNumber>(
            ceil(static_cast<double>(avg) / static_cast<double>(estimate_history_.size())));

        // reset trackers
        request_count_ = 0;

        mu_.unlock();

        // send to zipper
        Message zip_req;
        zip_req.type = ZIP_REQUEST;
        zip_req.shard_id = shard();
        zip_req.sender_id = id();
        zip_req.set_num_requests(slot_estimate_);

        Message resp;
        //cout << "req seq: " << zip_req.seq_or_count << endl;

        NetworkUtils::send_message_to_address(config_.zipper.first, config_.zipper.second, zip_req, resp, config_.max_retries);
    }

    void Proxy::send_out_batch() {
        // obtain lock
        mu_.lock();

        // don't send anything out if you don't have any sequence numbers
        if (next_send_ == 0) {
            //cout << "no sequences..." << endl;
            mu_.unlock();
            return;
        }

        // build message
        Message msg;
        msg.type = APPEND;
        msg.shard_id = shard();
        msg.sender_id = id();
        msg.set_sequence_number(sequences_.front());    // seq number

        // add commands to batch
        CommandBatch batch;

        if (!batch_values_.empty()) {
            // commands to send
            for (const auto& cmd : batch_values_) {
                batch.add_command(cmd);
            }
        } else {
            // no commands to send (send skip)
            msg.type = SKIP;
        }

        deque<int> clients_to_respond = client_sockets_;

        // clear pending commands/client sockets (they have now been handled)
        batch_values_.clear();
        client_sockets_.clear();
        sequences_.pop_front(); // removed used sequence number
        timeouts_.pop_front();

        if (timeouts_.size() > 0) next_send_ = timeouts_.front();
        else next_send_ = 0;

        mu_.unlock();

        msg.data = batch.serialize();

        // send out batch
        bool success = replicate_on_quorum(msg);

        // respond to clients
        Message resp;
        resp.type = SUCCESS;

        if (!success) {
            resp.type = FAILURE;
            cout << "[proxy " << id() << "] failed to replicate on quroum" << endl;
        }

        for (int client : clients_to_respond) {
            NetworkUtils::send_message(client, resp);
            close(client);
        }
    }
}}
