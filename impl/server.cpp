#include "server.h"
#include <future>

using namespace ziplog::api;
using std::future;

namespace ziplog {
namespace impl {

    Server::Server(const ServerConfig &cfg)
        : BaseNode<ServerConfig>(cfg)
    {
        // init lag
        lag_ = config_.epoch_duration_ms;

        // init subscriber worker threads
        for (size_t i = 0; i < config_.subscribers.size(); i++) {
            auto worker = std::make_unique<SubscriberWorker>();

            // insert into map
            {
                std::lock_guard<std::mutex> lock(subscriber_workers_mu_);
                subscriber_workers_[i] = std::move(worker);
            }

            // create thread
            subscriber_workers_[i]->worker_thread = std::make_unique<thread>(
                &Server::subscriber_worker_loop, this, i
            );
        }
        
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        start_listening();
        start_proxy_liveness_checks();
    }

    void Server::handle_connection(int proxy_socket) {
        while (running()) {
            // recv message from proxy
            Message msg;
            if (!NetworkUtils::recv_message(proxy_socket, msg)) {
                //std::cerr << "Failed to receive message from proxy" << std::endl;
                break;
            }
            //std::cout << "Server " << id_ << " received message from proxy " << msg.sender_id
              //        << " (seq: " << msg.seq_or_count << ", type: " << msg.type << ")" << std::endl;

            // process message from proxies
            if (msg.type == APPEND || msg.type == SKIP) {
                // store the message for durability
                NodeId id = msg.sender_id;
                mu_.lock();
                if (proxy_messages_.find(id) == proxy_messages_.end()) proxy_messages_[id] = deque<Message>();
                proxy_messages_[id].push_back(msg);
                mu_.unlock();

                // send ack
                Message ack_msg;
                ack_msg.type = ACK;
                ack_msg.shard_id = shard();
                ack_msg.sender_id = this->id();
                ack_msg.set_sequence_number(msg.get_sequence_number());
                NetworkUtils::send_message(proxy_socket, ack_msg);

                // begin replication
                std::thread([this, msg]() {
                    broadcast_to_subscribers(msg);
                }).detach();
            }

            else if (msg.type == ZIP_RESPONSE) {
                update_expected_proxy_timeouts(msg);

                Message ack_msg;
                ack_msg.type = ACK;
                ack_msg.shard_id = shard();
                ack_msg.sender_id = id();
                NetworkUtils::send_message(proxy_socket, ack_msg);
            }

            else if (msg.type == FREEZE) {
                handle_freeze(msg, true);
            }

            else if (msg.type == TRANSFER_REQUEST) {
                handle_transfer_request(proxy_socket, msg);
            }

            else if (msg.type == FREEZE_COMPLETE) {
                block_proxy(msg);
            }

            else if (msg.type == INCLUDE_PROXY) {
                introduce_proxy(msg);

                Message ack_msg;
                ack_msg.type = ACK;
                ack_msg.shard_id = shard();
                ack_msg.sender_id = id();
                NetworkUtils::send_message(proxy_socket, ack_msg);
            }

            else if (msg.type == INCLUDE_SUBSCRIBER) {
                // deserialize address
                string addr_info(msg.data.begin(), msg.data.end());
                size_t colon_pos = addr_info.find(':');
                string ip = addr_info.substr(0, colon_pos);
                int port = std::stoi(addr_info.substr(colon_pos + 1));
                Address subscriber = Address(ip, port);

                mu_.lock();
                config_.subscribers.push_back(Address(ip, port));
                unordered_map<NodeId, deque<Message>> messages_copy = proxy_messages_;
                mu_.unlock();

                Message ack_msg;
                ack_msg.type = ACK;
                ack_msg.shard_id = shard();
                ack_msg.sender_id = id();
                NetworkUtils::send_message(proxy_socket, ack_msg);

                // send all stored messages
                int sock = connection_pool_.get_connection(subscriber);
                if (sock >= 0) {
                    for (const auto& [proxy_id, messages] : messages_copy) {
                        if (is_blocked(proxy_id)) continue;

                        for (const auto& stored_msg : messages) {
                            Message fwd_msg = stored_msg;
                            fwd_msg.sender_id = id();
                            Message ack;

                            NetworkUtils::send_message(sock, stored_msg);
                            NetworkUtils::recv_message(sock, ack);
                        }
                    }
                }
            }
        }
        close(proxy_socket);
    }

    void Server::introduce_proxy(const Message& msg) {
        // obtain lock
        lock_guard<mutex> lock(mu_);

        // deserialize address
        string addr_info(msg.data.begin(), msg.data.end());
        size_t colon_pos = addr_info.find(':');
        string ip = addr_info.substr(0, colon_pos);
        int port = std::stoi(addr_info.substr(colon_pos + 1));

        NodeId proxy_id = msg.sender_id;

        if (!config_.isValidProxy(proxy_id)) {  // new proxy
            config_.proxies.push_back(Address(ip, port));
        } else {                                // rejoining proxy
            blocked_for_reconfiguration_.erase(proxy_id);
            proxy_timeouts_[proxy_id] = deque<Timestamp>();
            last_used_sequence_number_.erase(proxy_id);
        }
    }

    void Server::handle_freeze(const Message &msg, bool from_zipper) {
        cout << "[server " << id() << "] got a freeze" << endl;
        NodeId failed_proxy = msg.get_failed_proxy();

        // determine if the message is outdated
        mu_.lock();
        if (from_zipper && rounds_.find(failed_proxy) != rounds_.end() && rounds_[failed_proxy] >= msg.get_round()) {
            mu_.unlock();
            cout << "[server " << id() << "] outdated freeze" << endl;
            return;
        }

        rounds_[failed_proxy] = msg.get_round();
        blocked_for_reconfiguration_[failed_proxy] = false;

        // obtain lock and take note of last received message
        Message transfer;
        transfer.type = TRANSFER_REQUEST;
        transfer.shard_id = shard();
        transfer.sender_id = id();
        transfer.set_failed_proxy(failed_proxy);
        transfer.set_round(msg.get_round());
        transfer.set_sequence_number(last_used_sequence_number_[failed_proxy]);
        mu_.unlock();

        // bcast that to all other servers
        //cout << "[server " << id() << "] bcasting transfer" << endl;
        for (NodeId i = 0; i < config_.other_servers.size(); i++) {
            Address server = config_.other_servers[i];

            int sock = connection_pool_.get_connection(server);
            if (sock < 0) {
                //cout << "[server " << id() << "] failed to connect to server " << i << endl;
                continue;
            }

            if (!NetworkUtils::send_message(sock, transfer)) {
                //cout << "[server " << id() << "] failed to send transfer to server " << i << endl;
                continue;
            }

            while (true) {
                Message resp;
                if (!NetworkUtils::recv_message(sock, resp)) {
                    //cout << "[server " << id() << "] coudnlt recv on connection" << endl;
                    break;
                }
                if (resp.type == ACK) {
                    //cout << "[server " << id() << "] got ack from server " << i << endl;
                    break;
                }

                // store the message for durability
                mu_.lock();
                cout << "[server " << id() << "] got stored message w sequence " << resp.get_sequence_number() << endl;
                if (proxy_messages_.find(failed_proxy) == proxy_messages_.end()) proxy_messages_[failed_proxy] = deque<Message>();
                proxy_messages_[failed_proxy].push_back(resp);
                mu_.unlock();

                broadcast_to_subscribers(resp);
            }
        }

        // determine the latest
        mu_.lock();
        Message freeze_response;
        freeze_response.type = FREEZE_RESPONSE;
        freeze_response.shard_id = shard();
        freeze_response.sender_id = id();
        freeze_response.set_failed_proxy(failed_proxy);
        freeze_response.set_round(msg.get_round());
        freeze_response.set_sequence_number(last_used_sequence_number_[failed_proxy]);
        cout << "[server " << id() << "] passed data collection.last seq = " << last_used_sequence_number_[failed_proxy] << endl;
        mu_.unlock();

        int zip_sock = connection_pool_.get_connection(config_.zipper);
        if (zip_sock < 0) return;
        NetworkUtils::send_message(zip_sock, freeze_response);
    }

    void Server:: handle_transfer_request(int socket, const Message& msg) {
        // verify validity of sender (valid server)
        //if (msg.shard_id != shard() || !config_.isValidServer(msg.sender_id)) {
        if (msg.shard_id != shard()) {
            cout << "invalid server: " << msg.sender_id << endl;
            return;
        }

        NodeId failed_proxy = msg.get_failed_proxy();

        // determine if the message is outdated
        mu_.lock();
        if (rounds_.find(failed_proxy) != rounds_.end() && rounds_[failed_proxy] > msg.get_round()) {
            mu_.unlock();
            Message ack;
            ack.type = ACK;
            ack.shard_id = shard();
            ack.sender_id = id();
            NetworkUtils::send_message(socket, ack);
            return;
        }

        // update the round
        bool new_round = false;
        if (rounds_.find(failed_proxy) == rounds_.end() || rounds_[failed_proxy] < msg.get_round()) {
            rounds_[failed_proxy] = msg.get_round();
            new_round = true;
        }

        // send all messages you have
        SequenceNumber req_last_seq = msg.get_sequence_number();

        if (last_used_sequence_number_.find(failed_proxy) == last_used_sequence_number_.end()) {
            mu_.unlock();
            Message ack;
            ack.type = ACK;
            ack.shard_id = shard();
            ack.sender_id = id();
            ack.set_sequence_number(req_last_seq);
            NetworkUtils::send_message(socket, ack);
            return;
        }

        auto& messages = proxy_messages_[failed_proxy];
        mu_.unlock();

        for (const auto& stored_msg : messages) {
            SequenceNumber seq = stored_msg.get_sequence_number();
            if (seq <= req_last_seq) continue;

            if (!NetworkUtils::send_message(socket, stored_msg)) {
                return;
            }
        }

        Message ack;
        ack.type = ACK;
        ack.shard_id = shard();
        ack.sender_id = id();
        NetworkUtils::send_message(socket, ack);

        if (new_round) {
            handle_freeze(msg, false);
        }
    }

    void Server::broadcast_to_subscribers(const Message &msg) {
        // verify validity of sender (valid proxy)
        if (msg.shard_id != shard()) {
            cout << "invalid proxy: " << msg.sender_id << endl;
            return;
        }
        //cout << "[server " << id() << "] got message from proxy " << msg.sender_id << endl;

        // verify sender was not blocked for reconfiguration
        if (is_blocked(msg.sender_id)) {
            cout << "was blocked" << endl;
            return;
        }

        // update the last used timeout
        remove_timeout(msg);

        //cout << "Server broadcasting ---------------------------------" << endl;
        Message fwd_msg = msg;
        fwd_msg.sender_id = id();

        mu_.lock();
        vector<Address> subs_copy = config_.subscribers;
        mu_.unlock();

        vector<future<bool>> futures;
        for (size_t i = 0; i < subs_copy.size(); i++) {
            Address subscriber = subs_copy[i];

            futures.push_back(std::async(std::launch::async, [this, subscriber, fwd_msg, i]() {
                int sock = connection_pool_.get_connection(subscriber);
                if (sock < 0) {
                    //std::cerr << "Server " << id() << " failed to connect to subscriber " << i << std::endl;
                    return false;
                }

                if (!NetworkUtils::send_message(sock, fwd_msg)) {
                    //std::cerr << "Server " << id() << " failed to send to subscriber " << i << std::endl;
                    return false;
                }

                Message ack;
                if (!NetworkUtils::recv_message(sock, ack)) {
                    //std::cerr << "Server " << id() << " failed to recv ACK from subscriber " << i << std::endl;
                    return false;
                }

                return true;
            }));
        }

        size_t successful_sends = 0;
        for (auto& f : futures) {
            if (f.get()) successful_sends++;
        }

        cout << "[server " << id() << "] broadcast seq=" << msg.get_sequence_number() << " successful sends=" << successful_sends << "/" << num_subscribers() << endl;
    }

    bool Server::is_blocked(NodeId id) {
        // obtain lock
        lock_guard<mutex> lock(mu_);

        if (blocked_for_reconfiguration_.find(id) != blocked_for_reconfiguration_.end() && blocked_for_reconfiguration_[id]) return true;
        return false;
    }

    void Server::remove_timeout(const Message& msg) {
        // obtain lock
        lock_guard<mutex> lock(mu_);
        NodeId id = msg.sender_id;

        //cout << "[server " << id_ << "] heard from proxy " << id << endl;
        if (proxy_timeouts_[id].size() < 2) {
            cerr << "warning: proxy_timeouts_ too small for proxy " << id << endl;
            return;
        }

        if (blocked_for_reconfiguration_.find(id) == blocked_for_reconfiguration_.end()) {
            // remove timeout and sequence number
            proxy_timeouts_[id].pop_front();
            last_used_sequence_number_[id] = proxy_timeouts_[id].front();
            proxy_timeouts_[id].pop_front();
        } else {
            if (last_used_sequence_number_[id] < msg.get_sequence_number()) {
                last_used_sequence_number_[id] = msg.get_sequence_number();
            }
        }
    }

    void Server::update_expected_proxy_timeouts(const Message& msg) {
        // obtain lock
        lock_guard<mutex> lock(mu_);

        // update expected sequences (Timestamp, SequenceNumber, Timestamp, ...)
        proxy_timeouts_[msg.sender_id].insert(proxy_timeouts_[msg.sender_id].end(), msg.ordering_values.begin(), msg.ordering_values.end());
    }

    void Server::block_proxy(const Message& msg) {
        // obtain lock
        lock_guard<mutex> lock(mu_);
        cout << "[server " << id() << "] fully blocking proxy " << msg.get_failed_proxy() << endl;
        blocked_for_reconfiguration_[msg.get_failed_proxy()] = true;
    }

    void Server::shutdown() {
        BaseNode::shutdown();
        std::cout << "Server " << id() << " shutting down" << std::endl;
    }

    Server::~Server() {
        running_ = false;

        // shutdown subscriber worker threads
        {
            std::lock_guard<std::mutex> lock(subscriber_workers_mu_);
            for (auto& [idx, worker] : subscriber_workers_) {
                {
                    lock_guard<mutex> lock(worker->mu);
                    worker->shutdown = true;
                }
                worker->cv.notify_one();
            }
        }

        {
            std::lock_guard<std::mutex> lock(subscriber_workers_mu_);
            for (auto& [idx, worker] : subscriber_workers_) {
                if (worker->worker_thread && worker->worker_thread->joinable()) {
                    worker->worker_thread->join();
                }
            }
        }

        if (failure_detector_thread_.joinable()) {
            failure_detector_thread_.join();
        }
        connection_pool_.close_all();
        shutdown();
    }

    void Server::failure_detect() {
        running_ = true;

        while (running_) {
            Timestamp now = now_ms();
            for (NodeId id = 0; id < static_cast<NodeId>(num_proxies()); id++) {
                if (is_blocked(id)) continue;

                mu_.lock();
                if (blocked_for_reconfiguration_.find(id) == blocked_for_reconfiguration_.end() && !proxy_timeouts_[id].empty() && now >= proxy_timeouts_[id].front() + lag_) {
                    mu_.unlock();
                    cout << "[server " << this->id() << "] report proxy " << id << " time is " << now << " expected msg at " << proxy_timeouts_[id].front() + lag_ << endl;
                    report(id);
                } else {
                    mu_.unlock();
                }

            }

            std::this_thread::sleep_for(config_.epoch_duration_ms * 1ms);
        }
    }

    void Server::report(NodeId proxy_id) {
        cout << "[server " << id() << "] reporting proxy " << proxy_id << endl;
        // build and send report to zipper
        Message req;
        req.type = REPORT;
        req.shard_id = shard();
        req.sender_id = id();
        req.set_failed_proxy(proxy_id);

        int sock = connection_pool_.get_connection(config_.zipper);
        if (sock < 0) return;
        NetworkUtils::send_message(sock, req);
    }
}}
