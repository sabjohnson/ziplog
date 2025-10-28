#include "proxy.h"
#include <math.h>

using namespace ziplog::api;

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
        cur_sequences_size_ = 0;
        next_send_ = 0;


        start_listening();
        start_epochs();
    }

    void Proxy::shutdown() {
        BaseNode::shutdown();

        cout << "Proxy " << id() << " shutting down" << endl;
    }

    Proxy::~Proxy() {
        epoch_running_ = false;
        if (epoch_thread_.joinable()) {
            epoch_thread_.join();
        }

        lock_guard<mutex> lock(mu_);
        for (int client : client_sockets_) {
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
            handle_append(client_socket, req.data);
            return;
        } else if (req.type == ZIP_RESPONSE) {
            handle_zip_response(req);
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
        cout << "request count: " << request_count_ << endl;

        // take note of client socket (respond after its replicated during epoch interval)
        client_sockets_.push_back(client_socket);
    }

    // attempt to replicate on f + 1 storage servers
    bool Proxy::replicate_on_quorum(Message& msg) {
        size_t successful_sends = 0;
        for (size_t i = 0; i < config_.num_servers() && successful_sends < config_.quorum(); i++) {
            auto [server_ip, server_port] = config_.servers[i];

            if (NetworkUtils::send_message_to_address(server_ip, server_port, msg, config_.timeout_ms, config_.max_retries)) {
                successful_sends++;
            }
        }
        cout << "successful sends = " << successful_sends << " vs " << " quorum = " << config_.quorum() << endl;
        return successful_sends == config_.quorum();
    }

    void Proxy::handle_zip_response(Message& msg) {
        // obtain lock
        mu_.lock();

        // validate zip response
        if (msg.shard_id != shard()) return;

        // take note of number of allocated slots
        cur_sequences_size_ += msg.get_num_requests();
        cout << "proxy "  << id() << " got " << msg.get_num_requests() << " slots from the zipper" << endl;

        // add new sequence numbers to your pool
        sequences_.insert(sequences_.end(), msg.ordering_values.begin(), msg.ordering_values.end());

        // release lock
        mu_.unlock();

        // update next_send_update
        update_next_send();
    }

    /* -----------------------------------------------------------------------------------------------------------------------
        Epoch handling
     ----------------------------------------------------------------------------------------------------------------------- */

    void Proxy::epoch_timer() {
        epoch_running_ = true;
        epoch_startup_ = now_ms();
        update_next_send();

        while (epoch_running_) {
            Timestamp now = now_ms();
            Timestamp elapsed = now - epoch_startup_;

            if (elapsed >= config_.epoch_duration_ms) { // full epoch elapsed
                // restart state
                epoch_startup_ = now_ms();
                update_slot_estimate();
                update_next_send();
                cout << "[ proxy " << id() << "] new epoch for proxy" << endl;

            } else if (next_send_ != 0 && now >= next_send_) { // time to send out next batch
                cout << "[ proxy " << id() << "] updating send time for proxy" << endl;
                send_out_batch();
                update_next_send();
            }

            std::this_thread::sleep_for(5ms);
        }
    }

    void Proxy::update_slot_estimate() {
        // obtain lock
        lock_guard<mutex> lock(mu_);

        // update request history and calculate estimate for the appropriate number of epochs (i.e., min(total epochs, MAX_EPOCH_HISTORY))
        estimate_history_.push_back(request_count_);
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
        cout << "--------------------------- SLOT ESTIMATE" << endl;
        cout << "slot estimate: " << slot_estimate_ << endl;

        // send to zipper
        Message zip_req;
        zip_req.type = ZIP_REQUEST;
        zip_req.shard_id = shard();
        zip_req.sender_id = id();
        zip_req.set_num_requests(slot_estimate_);

        cout << "req seq: " << zip_req.seq_or_count << endl;

        NetworkUtils::send_message_to_address(config_.zipper.first, config_.zipper.second, zip_req, config_.timeout_ms, config_.max_retries);

        // reset trackers
        request_count_ = 0;
    }

    void Proxy::update_next_send() {
        // obtain lock
        lock_guard<mutex> lock(mu_);

        // return if we don't need to update yet
        Timestamp now = now_ms();
        if (next_send_ != 0 && now < next_send_) return;

        // get the next time we are working at
        Timestamp next = 0;
        if (cur_sequences_size_) {
            next = sequences_.front();  // get timestamp
            sequences_.pop_front(); // pop timestamp from sequences
        }
        next_send_ = next;

        if (!next_send_) {
            cout << "Zipper did not allocate slots for proxy " << id() << " this epoch" << endl;
        }
    }

    void Proxy::send_out_batch() {
        // obtain lock
        lock_guard<mutex> lock(mu_);
        cout << "[proxy " << id() << "] ---------------------------- sending skip? " << batch_values_.empty() << endl;

        // don't send anything out if you don't have any sequence numbers
        if (sequences_.empty()) {
            cout << "no sequences..." << endl;
            return;
        }


        // build message
        Message msg;
        msg.type = APPEND;
        msg.shard_id = shard();
        msg.sender_id = id();
        msg.seq_or_count = sequences_.front();

        cout << "sending out a batch with seq: " << msg.seq_or_count << endl;
        cout << "sequences: ";
        for (const auto& seq : sequences_) {
            cout << seq << ", ";
        }
        cout << endl;

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

        for (int client : client_sockets_) {
            NetworkUtils::send_message(client, resp);
            close(client);
        }

        // clear pending commands/client sockets (they have now been handled)
        sequences_.pop_front(); // removed used sequence number
        cur_sequences_size_--;  // update number of usable sequence numbers
        batch_values_.clear();
        client_sockets_.clear();
    }

}}
