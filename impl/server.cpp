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
        
        // set value of members (we already know ip addr is in our valid range based on parsed config)
        start_listening();
        start_proxy_liveness_checks();
    }

    void Server::handle_connection(int proxy_socket) {
        while (is_running_) {
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

                Message ack_msg;
                ack_msg.type = ACK;
                ack_msg.shard_id = shard();
                ack_msg.sender_id = id();
                NetworkUtils::send_message(proxy_socket, ack_msg);
            }

            else if (msg.type == INCLUDE_PROXY) {
                introduce_proxy(msg);
            }

            else if (msg.type == INCLUDE_SUBSCRIBER) {
                // deserialize address
                string addr_info(msg.data.begin(), msg.data.end());
                size_t colon_pos = addr_info.find(':');
                string ip = addr_info.substr(0, colon_pos);
                int port = std::stoi(addr_info.substr(colon_pos + 1));

               mu_.lock();
               config_.subscribers.push_back({ip, port});
               unordered_map<NodeId, deque<Message>> messages_copy = proxy_messages_;
               mu_.unlock();

                Message ack_msg;
                ack_msg.type = ACK;
                ack_msg.shard_id = shard();
                ack_msg.sender_id = id();
                NetworkUtils::send_message(proxy_socket, ack_msg);

                // send all stored messages
                for (const auto& [proxy_id, messages] : messages_copy) {
                    if (is_blocked(proxy_id)) continue;

                    for (const auto& stored_msg : messages) {
                        Message fwd_msg = stored_msg;
                        fwd_msg.sender_id = id();

                        Message ack;
                        NetworkUtils::send_message_to_address(ip, port, fwd_msg, ack, config_.max_retries);
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
            config_.proxies.push_back({ip, port});
        } else {                                // rejoining proxy
            blocked_for_reconfiguration_.erase(proxy_id);
            proxy_timeouts_[proxy_id] = deque<Timestamp>();
            last_used_sequence_number_.erase(proxy_id);
        }
    }

    void Server::handle_freeze(const Message &msg, bool from_zipper) {
        //cout << "[server " << id() << "] got a freeze" << endl;
        NodeId failed_proxy = msg.get_failed_proxy();

        // determine if the message is outdated
        mu_.lock();
        if (from_zipper && rounds_.find(failed_proxy) != rounds_.end() && rounds_[failed_proxy] >= msg.get_round()) {
            mu_.unlock();
            //cout << "[server " << id() << "] outdated freeze" << endl;
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
            const auto& [server_ip, server_port] = config_.other_servers[i];

            int socket = NetworkUtils::create_connector_socket();
            if (socket < 0) continue;

            if (!NetworkUtils::connect_to_address(socket, server_ip, server_port)) {
                close(socket);
                continue;
            }

            if (!NetworkUtils::send_message(socket, transfer)) {
                close(socket);
                continue;
            }

            while (true) {
                Message resp;
                if (!NetworkUtils::recv_message(socket, resp)) {
                    //cout << "[server " << id() << "] coudnlt recv on connection" << endl;
                    break;
                }
                if (resp.type == ACK) {
                    //cout << "[server " << id() << "] got ack from server " << i << endl;
                    break;
                }

                // store the message for durability
                mu_.lock();
                //cout << "[server " << id() << "] got stored message w sequence " << resp.get_sequence_number() << endl;
                if (proxy_messages_.find(failed_proxy) == proxy_messages_.end()) proxy_messages_[failed_proxy] = deque<Message>();
                proxy_messages_[failed_proxy].push_back(resp);
                mu_.unlock();

                broadcast_to_subscribers(resp);
            }
            close(socket);
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
        //cout << "[server " << id() << "] passed data collection.last seq = " << last_used_sequence_number_[failed_proxy] << endl;
        mu_.unlock();

        int sock = NetworkUtils::create_connector_socket();
        if (sock < 0) return;

        if (NetworkUtils::connect_to_address(sock, config_.zipper.ip, config_.zipper.port)) {
            NetworkUtils::send_message(sock, freeze_response);
        }
        close(sock);
    }

    void Server:: handle_transfer_request(int socket, const Message& msg) {
        // verify validity of sender (valid server)
        //if (msg.shard_id != shard() || !config_.isValidServer(msg.sender_id)) {
        if (msg.shard_id != shard()) {
            //cout << "invalid server: " << msg.sender_id << endl;
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
        cout << "[server " << id() << "] got message from proxy " << msg.sender_id
             << " data " << string(msg.data.begin(), msg.data.end()) << endl;

        // verify sender was not blocked for reconfiguration
        if (is_blocked(msg.sender_id)) {
            //cout << "was blocked" << endl;
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
            auto [subscriber_ip, subscriber_port] = subs_copy[i];

            futures.push_back(std::async(std::launch::async, [=]() {
                Message ack;
                if (!NetworkUtils::send_message_to_address(subscriber_ip, subscriber_port, fwd_msg, ack, config_.max_retries)) {
                    std::cerr << "Server " << id() << " failed to deliver to subscriber " << i << " after " << config_.max_retries << " attempts" << std::endl;
                    return false;
                }
                return true;
            }));
        }

        size_t successful_sends = 0;
        for (auto& f : futures) {
            if (f.get()) successful_sends++;
        }

        //cout << "[server " << id() << "] broadcast seq=" << msg.get_sequence_number() << " successful sends=" << successful_sends << "/" << num_subscribers() << endl;
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
        //cout << "[server " << id() << "] fully blocking proxy " << msg.get_failed_proxy() << endl;
        blocked_for_reconfiguration_[msg.get_failed_proxy()] = true;
    }

    void Server::shutdown() {
        BaseNode::shutdown();
        std::cout << "Server " << id() << " shutting down" << std::endl;
    }

    Server::~Server() {
        running_ = false;
        cv_.notify_all();

        if (failure_detector_thread_.joinable()) {
            //cout << "[server " << id() << "] trying to join" << endl;
            failure_detector_thread_.join();
            //cout << "[server " << id() << "] join complete" << endl;
        }
        shutdown();
    }

    void Server::failure_detect() {
        running_ = true;

        while (running_) {
            Timestamp now = now_ms();
            for (NodeId id = 0; id < static_cast<NodeId>(num_proxies()); id++) {
                if (!running_) break;
                if (is_blocked(id)) continue;

                mu_.lock();
                if (blocked_for_reconfiguration_.find(id) == blocked_for_reconfiguration_.end() && !proxy_timeouts_[id].empty() && now >= proxy_timeouts_[id].front() + lag_) {
                    mu_.unlock();
                    //cout << "[server " << this->id() << "] report proxy " << id << " time is " << now << " expected msg at " << proxy_timeouts_[id].front() + lag_ << endl;
                    report(id);
                } else {
                    mu_.unlock();
                }

            }

            //std::this_thread::sleep_for(config_.epoch_duration_ms * 1ms);
            std::unique_lock<mutex> lock(cv_mutex_);
            cv_.wait_for(lock, config_.epoch_duration_ms * 1ms, [this]() {
                return !running_;
            });
        }
        //cout <<"[server " << id() << "] failure_detetct() exitting" << endl;
    }

    void Server::report(NodeId proxy_id) {
        //cout << "[server " << id() << "] reporting proxy " << proxy_id << endl;
        auto [zipper_ip, zipper_port] = config_.zipper;

        // build and send report to zipper
        Message req;
        req.type = REPORT;
        req.shard_id = shard();
        req.sender_id = id();
        req.set_failed_proxy(proxy_id);

        Message resp;
        if (!NetworkUtils::send_message_to_address(zipper_ip, zipper_port, req, resp, config_.max_retries)) {
            //cout << "[server *] zipper did not pickup" << endl;
        }
    }
}}
