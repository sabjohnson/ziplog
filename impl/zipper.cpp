#include "zipper.h"
#include <algorithm>

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Zipper::Zipper(const ZiplogConfig& cfg) :
        BaseNode(0, cfg, cfg.zipper.first, cfg.zipper.second)
    {
        // global sequencing state
        num_proxies_ = cfg.num_proxies();
        global_seq_num_ = 0;

        start_listening();
        start_epochs();
    }

    void Zipper::handle_connection(int proxy_socket) {
        // read from and respond to valid request (shards match and know proxy id)
        Message req;
        if (!NetworkUtils::recv_message(proxy_socket, req)) {
            close(proxy_socket);
            return;
        }

        if (req.type == ZIP_REQUEST) {
            update_slot_estimate(req, proxy_socket);
        } else {
            close(proxy_socket);    // signifies completion of operation
        }
    }

    void Zipper::update_slot_estimate(Message &req, int proxy_socket) {
        // validate request
        if (req.shard_id != shard() || !config_.isValidProxy(req.sender_id)) {
            return;
        }
        std::cout << "Received request from proxy " << req.sender_id
                  << " with " << req.ordering_values.size() << " timestamp(s)" << std::endl;

        // obtain lock
        lock_guard<mutex> lock(mu_);

        // take note of batch request
        pending_requests_[req.sender_id] = {req.sender_id, req.ordering_values, proxy_socket};
    }

    void Zipper::shutdown() {
        BaseNode::shutdown();
        std::cout << "Zipper shutting down" << std::endl;
    }

    Zipper::~Zipper() {
        epoch_running_ = false;
        shutdown();
        if (epoch_thread_.joinable()) {
            epoch_thread_.join();
        }
    }

    void Zipper::epoch_timer() {
        epoch_running_ = true;
        epoch_startup_ = now_ms();

        while (epoch_running_) {
            Timestamp now = now_ms();
            Timestamp elapsed = now - epoch_startup_;

            const Timestamp allocation_time = (EPOCH_DURATION_MS * 3) / 4;

            if (elapsed >= allocation_time && elapsed < (allocation_time + 10)) {
                // allocate slots at 3/4 point
                allocate_slots();
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
            resp.shard_id = shard();
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