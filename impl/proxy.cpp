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

        // Initialize server worker threads
         for (size_t i = 0; i < config_.servers.size(); i++) {
            // create object
            auto worker = std::make_unique<ServerWorker>();

            // insert into map
            {
                std::lock_guard<std::mutex> lock(server_workers_mu_);
                server_workers_[i] = std::move(worker);
            }

            // create thread
            server_workers_[i]->worker_thread = thread(&Proxy::server_worker_loop, this, i);
        }

        start_listening();
        start_epochs();
    }

    Proxy::Proxy(const ProxyConfig &cfg, bool registered)
        : BaseNode<ProxyConfig>(cfg)
    {
        if (!registered) {
            registered_ = registered;
        }

        // Initialize server worker threads
         for (size_t i = 0; i < config_.servers.size(); i++) {
            // create object
            auto worker = std::make_unique<ServerWorker>();

            // insert into map
            {
                std::lock_guard<std::mutex> lock(server_workers_mu_);
                server_workers_[i] = std::move(worker);
            }

            // create thread
            server_workers_[i]->worker_thread = thread(&Proxy::server_worker_loop, this, i);
        }

        attempt_join(true);
        start_listening();
    }

    Proxy::~Proxy() {
        cout << "Proxy " << id() << " shutting down" << endl;
        // shutdown server workers
        {
            std::lock_guard<std::mutex> lock(server_workers_mu_);
            for (auto& [idx, worker] : server_workers_) {
                {
                    lock_guard<mutex> lock(worker->queue_mu);
                    worker->shutdown = true;
                }
                worker->cv.notify_one();
            }
        }

        {
            std::lock_guard<std::mutex> lock(server_workers_mu_);
            for (auto& [idx, worker] : server_workers_) {
                if (worker->worker_thread.joinable()) {
                    worker->worker_thread.join();
                }
            }
        }

        epoch_running_ = false;
        if (epoch_thread_.joinable()) {
            epoch_thread_.join();
        }

        zipper_connection_pool_.close_all();
        //shutdown();
    }

    /* -----------------------------------------------------------------------------------------------------------------------
        Message Passing
     ----------------------------------------------------------------------------------------------------------------------- */

    void Proxy::handle_connection(int client_socket) {
        while (running() && registered_) {
            // read from and respond to valid request
            Message req;
            if (!NetworkUtils::recv_message(client_socket, req)) {
                //close(client_socket);
                break;
            }

            // build and send response
            Message resp;

            if (req.type == APPEND) {
                if (!registered_) {
                    cout << "[proxy " << id() << "] not registered yet" << endl;
                    Message resp;
                    resp.type = FAILURE;
                    NetworkUtils::send_message(client_socket, resp);
                    //close(client_socket);
                    continue;
                }
                cout << "[proxy " << id() << "] ------------------------------------- RECV CLIENT REQ" << endl;
                cout << "[proxy " << id() << "] New client connected on socket " << client_socket << endl;
                // create pending request
                PendingRequest pr(req.data, client_socket);

                // add to client's buffer ((blocking operation)
                if (!client_buffers_[client_socket].push(pr)) {
                    // This shouldn't happen with blocking push, but handle anyway
                    cout << "[proxy " << id() << "] Failed to push to buffer (interrupted?)" << endl;
                    Message resp;
                    resp.type = FAILURE;
                    NetworkUtils::send_message(client_socket, resp);
                    continue;
                }

                cout << "[proxy " << id() << "] Successfully pushed to buffer. Buffer size: " << client_buffers_[client_socket].size() << endl;

                {
                    lock_guard<mutex> lock(epoch_mu_);
                    request_count_++;
                }
            }

            else if (req.type == ZIP_RESPONSE) {
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
        }
        close(client_socket);
        {
            lock_guard<mutex> lock(buffers_mu_);
            client_buffers_.erase(client_socket);
        }
        cout << "[proxy " << id() << "] Closed connection to client socket " << client_socket << endl;
    }

/*
    void Proxy::handle_append(int client_socket, const Command& data) {
        // obtain lock
        lock_guard<mutex> lock(epoch_mu_);

        // take notes of command
        batch_values_.push_back(data);

        // increment number of requests
        request_count_++;
        //cout << "request count: " << request_count_ << endl;

        // take note of client socket (respond after its replicated during epoch interval)
        client_sockets_.push_back(client_socket);
    }
*/

    bool Proxy::replicate_on_quorum(Message& msg) {
        cout << "replicate on quorum ------------------------------------" << msg.type << endl;
        auto state = std::make_shared<ReplicationState>();
        state->seq = msg.get_sequence_number();

        // push message to queues of server workers
        {
            std::lock_guard<std::mutex> lock(server_workers_mu_);
            cout << "[proxy " << id() << "] replicate_on_quorum: server_workers_.size() = " << server_workers_.size() << endl;
            for (auto& [idx, worker] : server_workers_) {
                {
                    lock_guard<mutex> lock(worker->queue_mu);
                    worker->pending_queue.push({msg, state});
                }
                worker->cv.notify_one();
            }
        }

        // wait for f + 1
        {
            std::unique_lock<mutex> lock(state->mu);
            state->cv.wait(lock, [&]() {
                cout << "replicate = " << (state->ack_count >= static_cast<int>(quorum())) << endl;
                return state->ack_count >= static_cast<int>(quorum());
            });
        }

        return true;
    }

/*
    // attempt to replicate on f + 1 storage servers
    bool Proxy::replicate_on_quorum1(Message& msg) {
        vector<future<bool>> futures;

        for (size_t i = 0; i < num_servers(); i++) {
            Address server = config_.servers[i];

            futures.push_back(std::async(std::launch::async, [this, server, msg, i]() {
                int sock = connection_pool_.get_connection(server);
                if (sock < 0) {
                    //std::cerr << "[proxy " << id() << "] failed to connect to server " << i << std::endl;
                    return false;
                }

                if (!NetworkUtils::send_message(sock, msg)) {
                    //std::cerr << "[proxy " << id() << "] failed to send to server " << i << std::endl;
                    connection_pool_.close_connection(server);
                    return false;
                }

                Message ack;
                if (!NetworkUtils::recv_message(sock, ack)) {
                    //std::cerr << "[proxy " << id() << "] failed to recv ack from server " << i << std::endl;
                    connection_pool_.close_connection(server);
                    return false;
                }

                return true;
            }));
        }

        size_t successful_sends = 0;
        for (auto& f : futures) {
            if (f.get()) successful_sends++;
        }

        cout << "successful sends = " << successful_sends << " vs " << " quorum = " << quorum() << endl;
        return successful_sends >= quorum();
    }
*/
    void Proxy::handle_zip_response(Message& msg) {
        // validate zip response
        if (msg.shard_id != shard()) return;

        // obtain lock
        lock_guard<mutex> lock(epoch_mu_);

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

        string addr_info = address().ip + ":" + std::to_string(address().port);
        req.data = Command(addr_info.begin(), addr_info.end());

        int sock = zipper_connection_pool_.get_connection(config_.zipper);
        if (sock < 0) return;
        NetworkUtils::send_message(sock, req);
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
        epoch_mu_.lock();

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

        epoch_mu_.unlock();

        // send to zipper
        Message zip_req;
        zip_req.type = ZIP_REQUEST;
        zip_req.shard_id = shard();
        zip_req.sender_id = id();
        zip_req.set_num_requests(slot_estimate_);

        Message resp;
        cout  << "[proxy " << id() << "] req seq: " << zip_req.seq_or_count << endl;

        int sock = zipper_connection_pool_.get_connection(config_.zipper);
        if (sock < 0) {
            cout << "could not send to zipper" << endl;
            return;
        }
        if (NetworkUtils::send_message(sock, zip_req)) {
            cout << "sent to zipper" << endl;
        } else {
            cout << "could not send to zipper" << endl;
        }
    }

    void Proxy::send_out_batch() {
        // obtain lock
        epoch_mu_.lock();

        // don't send anything out if you don't have any sequence numbers
        if (next_send_ == 0 || sequences_.empty()) {
            cout << "no sequences..." << endl;
            epoch_mu_.unlock();
            return;
        }

        // build message
        Message msg;
        msg.shard_id = shard();
        msg.sender_id = id();
        msg.set_sequence_number(sequences_.front());    // seq number

        // clear pending commands/client sockets (they have now been handled)
        sequences_.pop_front(); // removed used sequence number
        timeouts_.pop_front();

        if (timeouts_.size() > 0) next_send_ = timeouts_.front();
        else next_send_ = 0;

        epoch_mu_.unlock();

        // add commands to batch
        CommandBatch batch;
        set<int> participating_clients;
        const size_t MAX_BATCH_BYTES = 60000;
        size_t current_batch_size = 0;

        {
            lock_guard<mutex> lock(buffers_mu_);
            bool pulled = true;
            while (pulled && current_batch_size < MAX_BATCH_BYTES) {
                pulled = false;
                // round-robin: pop one request from each client buffer
                for (auto& [socket, buffer] : client_buffers_) {
                    if (current_batch_size >= MAX_BATCH_BYTES) break;

                    PendingRequest pr;
                    if (buffer.peek(pr)) {
                        if (current_batch_size + pr.cmd.size() <= MAX_BATCH_BYTES) {
                            buffer.pop(pr);
                            batch.add_command(pr.cmd);
                            participating_clients.insert(pr.client_socket);
                            current_batch_size += pr.cmd.size();
                            pulled = true;
                        }
                    }
                }
            }
        }


        if (participating_clients.empty()) {
            msg.type = SKIP;
            msg.data = Command();
        } else {
            msg.type = APPEND;
            msg.data = batch.serialize();
        }

        msg.data = batch.serialize();

        // send out batch
        bool success = replicate_on_quorum(msg);

        // respond to clients
        Message resp;
        resp.type = success ? SUCCESS : FAILURE;

        if (!success) {
            resp.type = FAILURE;
            cout << "[proxy " << id() << "] failed to replicate on quroum" << endl;
        } else {
            cout << "[proxy " << id() << "] success in replicating on quroum" << endl;
        }

        cout << "[proxy " << id() << "] participating clients size = " << participating_clients.size() << endl;
        for (int client : participating_clients) {
            NetworkUtils::send_message(client, resp);
        }
    }
}}
