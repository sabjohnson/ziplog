#include "zipper.h"
#include "network_utils.h"
#include <sys/socket.h>     // accept()
#include <netinet/in.h>     // sockaddr_in
#include <unistd.h>
#include <algorithm>

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Zipper::Zipper(const ZiplogConfig& cfg) {
        config_ = cfg;
        shard_id_ = 0;
        auto [ip, p] = cfg.zipper;
        ip_address_ = ip;
        port_ = p;
        is_running_ = false;
        
        zipper_sock_ = -1;

        // global sequencing state
        num_proxies_ = cfg.num_proxies();
        global_seq_num_ = 0;

        running_thread_ = thread(&Zipper::run, this);
        epoch_thread_ = thread(&Zipper::epoch_timer, this);  // inits epoch_startup_

    }

    void Zipper::run() {
        if (is_running_) {
            std::cerr << "Zipper already running" << std::endl;
            return;
        }
        is_running_ = true;

        // create listening socket
        zipper_sock_ = NetworkUtils::create_listening_socket(ip_address_, port_, true);
        if (zipper_sock_ < 0) {
            std::cerr << "Zipper failed to create server socket" << std::endl;
            return;
        }

        // handle connections
        while (is_running_) {
            struct sockaddr_in proxy_addr;
            socklen_t proxy_len = sizeof(proxy_addr);

            // accept connection
            int proxy_socket = accept(zipper_sock_, (struct sockaddr*)&proxy_addr, &proxy_len);
            if (proxy_socket < 0) {
                if (is_running_) {
                    std::cerr << "Failed to accept connection" << std::endl;
                    continue;
                } else {
                    break;
                }
            }

            // handle request in thread
            thread proxy_thread(&Zipper::handle_proxy, this, proxy_socket);
            proxy_thread.detach();
        }
    }

    void Zipper::handle_proxy(int proxy_socket) {
        // read from and respond to valid request (shards match and know proxy id)
        Message req;
        if (!NetworkUtils::recv_message(proxy_socket, req)) {
            close(proxy_socket);
            return;
        }

        std::cout << "Received request from proxy " << req.sender_id
                  << " with " << req.ordering_values.size() << " timestamp(s)" << std::endl;

        if (req.type == ZIP_REQUEST && req.shard_id == shard_id_ && req.sender_id < static_cast<NodeId>(num_proxies_)) {
            // obtain lock
            lock_guard<mutex> lock(mu_);

            // take note of batch request
            pending_requests_[req.sender_id] = {req.sender_id, req.ordering_values, proxy_socket};

            // timer will handle the response
        } else {
            close(proxy_socket);    // signifies completion of operation
        }
    }

    void Zipper::shutdown() {
        is_running_ = false;
        if (zipper_sock_ >= 0) {
            ::shutdown(zipper_sock_, SHUT_RDWR);
            close(zipper_sock_);
            zipper_sock_ = -1;
        }
        std::cout << "Zipper shutting down" << std::endl;
    }

    Zipper::~Zipper() {
        shutdown();
        if (running_thread_.joinable()) {
            running_thread_.join();  // wait for thread to finish
        }

        if (epoch_thread_.joinable()) {
            epoch_thread_.join();
        }
    }

    void Zipper::epoch_timer() {
        epoch_startup_ = now_ms();

        while (is_running_) {
            Timestamp now = now_ms();
            Timestamp elapsed = now - epoch_startup_;

            const Timestamp allocation_time = (EPOCH_DURATION_MS * 3) / 4;

            if (elapsed >= allocation_time && elapsed < (allocation_time + 10)) {
                // allocate slots at 3/4 point
                Zipper::allocate_slots();
                std::this_thread::sleep_for(10ms);
            }

            if (elapsed >= EPOCH_DURATION_MS) {
                // restart timer
                epoch_startup_ = now_ms();
            }

            std::this_thread::sleep_for(5ms);
        }
    }

    void Zipper::allocate_slots() {
        // obtain lock
        lock_guard<mutex> lock(mu_);

        std::cout << "Allocating slots for " << pending_requests_.size() << " requests" << std::endl;

        if (pending_requests_.empty()) return;

        unordered_map<NodeId, int> proxy_sockets;  // id > socket
        vector<pair<Timestamp, NodeId>> timestamps;    // vector or {timestamp, proxy_id}

        for (const auto& [proxy_id, batch] : pending_requests_) {
            // store connector socket
            proxy_sockets[proxy_id] = batch.proxy_socket;

            // add all timestamps
            for (const auto& timestamp : batch.ordering_values) {
                timestamps.push_back({timestamp, proxy_id});
            }
        }

        // sort timestamps
        std::sort(timestamps.begin(), timestamps.end());

        // allocate seq numbers
        unordered_map<NodeId, vector<SequenceNumber>> proxy_sequence_numbers;
        for (const auto& p : timestamps) {
            proxy_sequence_numbers[p.second].push_back(global_seq_num_++);
        }

        std::cout << "Allocated slots ------------------------------------------------------" << std::endl;

        // respond to all in this epoch
        for (const auto& [proxy_id, values] : proxy_sequence_numbers) {
            // get proxy socket
            int recipient = proxy_sockets[proxy_id];

            Message resp;
            resp.type = ZIP_RESPONSE;
            resp.shard_id = shard_id_;
            resp.set_num_requests(static_cast<SequenceNumber>(values.size()));
            resp.set_assigned_sequences(values);

            NetworkUtils::send_message(recipient, resp);
            close(recipient);

            std::cout << "proxy " << proxy_id << " : [";
            for (size_t i = 0; i < values.size(); ++i) {
                std::cout << values[i];
                if (i < values.size() - 1) std::cout << ", ";
            }
            std::cout << "]" << std::endl;
        }
        std::cout << "----------------------------------------------------------------------" << std::endl;

        // clear batch requests for this epoch
        pending_requests_.clear();
    }
}}