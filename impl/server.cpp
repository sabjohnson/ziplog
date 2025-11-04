#include "server.h"
#include <future>

using namespace ziplog::api;
using std::future;

namespace ziplog {
namespace impl {

    Server::Server(NodeId server_id, const ZiplogConfig &cfg)
        : BaseNode(server_id, cfg, cfg.servers[server_id].first, cfg.servers[server_id].second)
    {
        // validate node id
        validate_node_id(server_id, cfg.num_servers(), "Server");

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
                broadcast_to_subscribers(msg);
            }

            else if (msg.type == ZIP_RESPONSE) {
                update_expected_proxy_timeouts(msg);
            }

            else if (msg.type == FREEZE) {
                handle_freeze(proxy_socket, msg);
                return;
            }

            else if (msg.type == RECONFIGURATION) {
                block_proxy(msg);
            }
            
            // send response (default to ack for now)
            Message ack_msg;
            ack_msg.type = ACK;
            ack_msg.shard_id = shard();
            ack_msg.sender_id = id_;
            ack_msg.seq_or_count = msg.seq_or_count;
            
            if (!NetworkUtils::send_message(proxy_socket, ack_msg)) {
                //std::cerr << "Failed to send ACK to proxy" << std::endl;
                // break here?
            } else {
                //std::cout << "Server " << id_ << " sent ACK for message " << msg.seq_or_count << "\n" << std::endl;
            }
        }
        close(proxy_socket);
    }

    void Server::handle_freeze(int proxy_socket, const Message &msg) {
        block_proxy(msg);

        // obtain lock
        mu_.lock();
        Message ack_msg;
        ack_msg.type = ACK;
        ack_msg.shard_id = shard();
        ack_msg.sender_id = id();
        ack_msg.set_sequence_number(last_used_sequence_number_[msg.get_failed_proxy()]);
        mu_.unlock();

        if (!NetworkUtils::send_message(proxy_socket, ack_msg)) {
            std::cerr << "Failed to send report repsonse to zipper" << std::endl;
            // break here?
        } else {
            cout << "Server " << id() << " report responseto zipper" << endl;
        }

    }

    void Server::broadcast_to_subscribers(const Message &msg) {
        // verify validity of sender (valid proxy)
        if (msg.shard_id != shard() || !config_.isValidProxy(msg.sender_id)) {
            cout << "invalid proxy: " << msg.sender_id << endl;
            return;
        }

        // verify sender was not blocked for reconfiguration
        if (is_blocked(msg.sender_id)) {
            return;
        }

        remove_timeout(msg.sender_id);

        //cout << "Server broadcasting ---------------------------------" << endl;
        Message fwd_msg = msg;
        fwd_msg.sender_id = id();

        vector<future<void>> futures;
        for (size_t i = 0; i < config_.num_subscribers(); i++) {
            auto [subscriber_ip, subscriber_port] = config_.subscribers[i];

            futures.push_back(std::async(std::launch::async, [=]() {
                Message ack;
                if (!NetworkUtils::send_message_to_address(subscriber_ip, subscriber_port, fwd_msg, ack, config_.max_retries)) {
                    std::cerr << "Server " << id_ << " failed to deliver to subscriber " << i << " after " << config_.max_retries << " attempts" << std::endl;
                }
            }));
        }

        for (auto& f : futures) {
            f.wait();
        }
    }

    bool Server::is_blocked(NodeId id) {
        // obtain lock
        lock_guard<mutex> lock(mu_);

        if (blocked_for_reconfiguration_.find(id) != blocked_for_reconfiguration_.end()) return true;
        return false;
    }

    void Server::remove_timeout(NodeId id) {
        // obtain lock
        lock_guard<mutex> lock(mu_);

        //cout << "[server " << id_ << "] heard from proxy " << id << endl;
        if (proxy_timeouts_[id].size() < 2) {
            cerr << "warning: proxy_timeouts_ too small for proxy " << id << endl;
            return;
        }

        // remove timeout and sequence number
        proxy_timeouts_[id].pop_front();
        last_used_sequence_number_[id] = proxy_timeouts_[id].front();
        proxy_timeouts_[id].pop_front();
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

        blocked_for_reconfiguration_[msg.sender_id] = true;
    }

    void Server::shutdown() {
        BaseNode::shutdown();
        std::cout << "Server " << id_ << " shutting down" << std::endl;
    }

    Server::~Server() {
        running_ = false;
        if (failure_detector_thread_.joinable()) {
            failure_detector_thread_.join();
        }
        shutdown();
    }

    void Server::failure_detect() {
        running_ = true;

        while (running_) {
            Timestamp now = now_ms();
            for (NodeId id = 0; id < config_.num_proxies(); id++) {
                if (is_blocked(id)) continue;

                mu_.lock();
                if (!proxy_timeouts_[id].empty() && now >= proxy_timeouts_[id].front() + lag_) {
                    mu_.unlock();
                    report(id);
                } else {
                    mu_.unlock();
                }

            }

            std::this_thread::sleep_for(config_.epoch_duration_ms * 1ms);
        }
    }

    void Server::report(NodeId id) {
        cout << "[server " << id_ << "] reporting proxy " << id << endl;
        auto [zipper_ip, zipper_port] = config_.zipper;

        // build and send report to zipper
        Message req;
        req.type = REPORT;
        req.shard_id = shard();
        req.sender_id = id_;
        req.set_failed_proxy(id);

        Message resp;
        NetworkUtils::send_message_to_address(zipper_ip, zipper_port, req, resp, config_.max_retries);
    }
}}
