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

    Proxy::Proxy(const ProxyConfig &cfg)
        : BaseNode<ProxyConfig>(cfg)
    {
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        request_count_ = 0;
        next_send_ = 0;
        epoch_startup_ = 0;
        epoch_running_ = false;

        start_listening();
        start_epochs();
    }

    Proxy::Proxy(const ProxyConfig &cfg, bool registered)
        : BaseNode<ProxyConfig>(cfg)
    {
        if (!registered) {
            registered_ = registered;
        }
        request_count_ = 0;
        next_send_ = 0;
        epoch_startup_ = 0;
        epoch_running_ = false;

        attempt_join(true);
        start_listening();
    }

    void Proxy::shutdown() {
        BaseNode::shutdown();
    }

    Proxy::~Proxy() {
        epoch_running_ = false;

        {
        std::lock_guard<std::mutex> lock(epoch_cv_mutex_);
        epoch_cv_.notify_all();
        }

        if (epoch_thread_.joinable()) {
            //cout << "[proxy " << id() << "] joinging epoch thread" << endl;
            epoch_thread_.join();
            //cout << "[proxy " << id() << "] epoch thread joined" << endl;
        }

        mu_.lock();
        Message failure;
        failure.type = FAILURE;
        for (int client : client_sockets_) {
            //cout << "[proxy " << id() << "] sending failure to client on shutdown" << endl;
            NetworkUtils::send_message(client, failure);
            close(client);
        }
        mu_.unlock();

        shutdown();
        //cout << "[proxy " << id() << "] shutting down" << endl;
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

        // build and send response
        Message resp;
        resp.type = ACK;

        if (req.type == APPEND) {
            if (registered_) {
                handle_append(client_socket, req.data);
                cout << "[proxy " << id() << "] ------------------------------------- RECV CLIENT REQ" << endl;
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
            close(client_socket);
            return;
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
        cout << "[proxy " << id() << "] request count: " << request_count_ << endl;

        // take note of client socket (respond after its replicated during epoch interval)
        client_sockets_.push_back(client_socket);
    }

    // attempt to replicate on f + 1 storage servers
    bool Proxy::replicate_on_quorum(Message& msg) {
        vector<future<bool>> futures;

        for (size_t i = 0; i < num_servers(); i++) {
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

        //cout << "successful sends = " << successful_sends << " vs " << " quorum = " << quorum() << endl;
        return successful_sends >= quorum();
    }

    void Proxy::handle_zip_response(Message& msg) {
        // obtain lock
        lock_guard<mutex> lock(mu_);

        // validate zip response
        if (msg.shard_id != shard()) return;

        //cout << "[proxy "  << id() << "] got " << msg.get_num_requests() << " slots from the zipper" << endl;

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

        string addr_info = address().ip + ":" + std::to_string(address().port);
        req.data = Command(addr_info.begin(), addr_info.end());

        int sock = NetworkUtils::create_connector_socket();
        if (sock < 0) return;

        if (NetworkUtils::connect_to_address(sock, config_.zipper.ip, config_.zipper.port)) {
            NetworkUtils::send_message(sock, req);
        }
    }

    /* -----------------------------------------------------------------------------------------------------------------------
        Epoch handling
     ----------------------------------------------------------------------------------------------------------------------- */

    void Proxy::epoch_timer() {
        //cout << "[proxy " << id() << "] calling epoch_timer " << endl;
        epoch_running_ = true;
        epoch_startup_ = now_ms();
        const Timestamp sleep_duration = std::max(static_cast<Timestamp>(1), config_.epoch_duration_ms / 200);

        while (epoch_running_) {
            //cout << "[proxy " << id() << "] epoch_running_ " << endl;
            Timestamp now = now_ms();
            Timestamp elapsed = now - epoch_startup_;

            if (registered_) {
                //cout << "[proxy " << id() << "] is registered" << endl;
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

            //std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
            std::unique_lock<mutex> lock(epoch_cv_mutex_);
            if (epoch_cv_.wait_for(lock, std::chrono::milliseconds(sleep_duration), [this]() {
                //cout << "[proxy " << id() << "] checking condition" << endl;
                return !epoch_running_.load();
            })) {
                // Predicate returned true, exit immediately
                //cout << "[proxy " << id() << "] condition met, exiting" << endl;
                break;
            }
            //cout << "[proxy " << id() << "] done waiting" << endl;
        }
        //cout << "[proxy " << id() << "] epoch_timer() exiting" << endl;
    }

    void Proxy::update_slot_estimate() {
        //cout << "[proxy " << id() << "update_slots_estimate() waiting for lock" << endl;
        // obtain lock
        mu_.lock();

        // update request history and calculate estimate for the appropriate number of epochs (i.e., min(total epochs, MAX_EPOCH_HISTORY))
        estimate_history_.push_back(request_count_);
        //cout << "request count = " << request_count_ << endl;
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
        //cout << "[proxy " << id() << " update_slots_estimate() releasing lock" << endl;

        // send to zipper
        Message zip_req;
        zip_req.type = ZIP_REQUEST;
        zip_req.shard_id = shard();
        zip_req.sender_id = id();
        zip_req.set_num_requests(slot_estimate_);

        Message resp;
        //cout << "req seq: " << zip_req.seq_or_count << endl;
        int sock = NetworkUtils::create_connector_socket();
        if (sock < 0) return;

        if (NetworkUtils::connect_to_address(sock, config_.zipper.ip, config_.zipper.port)) {
            NetworkUtils::send_message(sock, zip_req);
            //cout << "sent to zipper" << endl;
        } else {
            //cout << "could not send to zipper" << endl;
        }
        close(sock);
        //cout << "[proxy " << id() << "update_slot_estimates() exitting" << endl;
    }

    void Proxy::send_out_batch() {
        cout << "[proxy " << id() << "sned_out_batch() waiting for lock" << endl;

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
                cout << command_to_string(cmd) << endl;
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
        //cout << "[proxy " << id() << "send_out_batch() releasing lock" << endl;

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
        cout << "[proxy " << id() << "send_out_batch() returning" << endl;
    }
}}
