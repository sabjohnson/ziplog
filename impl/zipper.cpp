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

        // initialize proxy estimate tracker
        for (NodeId i = 0; i < num_proxies_; i++) {
            proxy_estimates_[i] = 0;
        }

        // start running
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
            update_slot_estimate(req);
        }

        Message resp;
        resp.type = ACK;
        NetworkUtils::send_message(proxy_socket, resp);
        close(proxy_socket);
    }

    void Zipper::update_slot_estimate(Message &req) {
        // validate request
        if (req.shard_id != shard() || !config_.isValidProxy(req.sender_id)) {
            return;
        }

        if (req.get_num_requests()) {
            cout << "Received request from proxy " << req.sender_id << " with " << req.get_num_requests() << " timestamp(s)" << endl;
        }

        // obtain lock
        lock_guard<mutex> lock(mu_);

        // take note of number fo requests
        proxy_estimates_[req.sender_id] = req.get_num_requests();
    }

    void Zipper::shutdown() {
        BaseNode::shutdown();
        cout << "Zipper shutting down" << endl;
    }

    Zipper::~Zipper() {
        epoch_running_ = false;
        if (epoch_thread_.joinable()) {
            epoch_thread_.join();
        }
        shutdown();
    }

    void Zipper::epoch_timer() {
        epoch_running_ = true;
        epoch_startup_ = now_ms();

        const Timestamp allocation_time = (EPOCH_DURATION_MS * 3) / 4;

        while (epoch_running_) {
            Timestamp now = now_ms();
            Timestamp elapsed = now - epoch_startup_;

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

        cout << "---------------------------PROXY ESTIMATES" << endl;
        for (auto& [proxy_id, est] : proxy_estimates_) {
            cout << "proxy " << proxy_id << ": " << est << endl;
        }

        vector<pair<double, NodeId>> timestamps;    // vector or {timestamp, proxy_id}

        for (const auto& [proxy_id, estimate] : proxy_estimates_) {
            if (estimate == 0) continue;

            double interval = EPOCH_DURATION_MS / estimate;
            double time_point = interval / 2;
            int count = estimate;

            while (count) {
                timestamps.push_back({time_point, proxy_id});
                time_point += interval;
                count--;
            }
        }

        // sort timestamps
        std::sort(timestamps.begin(), timestamps.end());

        // allocate seq numbers
        unordered_map<NodeId, vector<SequenceNumber>> proxy_sequence_numbers;
        for (const auto& p : timestamps) {
            proxy_sequence_numbers[p.second].push_back(global_seq_num_++);
        }

        // respond to all in this epoch
        for (const auto& [proxy_id, values] : proxy_sequence_numbers) {

            Message resp;
            resp.type = ZIP_RESPONSE;
            resp.shard_id = shard();
            resp.set_num_requests(static_cast<SequenceNumber>(values.size()));
            resp.set_assigned_sequences(values);

            auto& [proxy_ip, proxy_port] = config_.proxies[proxy_id];
            NetworkUtils::send_message_to_address(proxy_ip, proxy_port, resp, config_.timeout_ms, config_.max_retries);

            std::cout << "proxy " << proxy_id << " : [";
            for (size_t i = 0; i < values.size(); ++i) {
                std::cout << values[i];
                if (i < values.size() - 1) std::cout << ", ";
            }
            std::cout << "]" << std::endl;
        }
    }
}}