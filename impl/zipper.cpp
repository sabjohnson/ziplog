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

        if (req.type == REPORT) {
            request_last_messages(req);
        }

        Message resp;
        resp.type = ACK;
        NetworkUtils::send_message(proxy_socket, resp);
        close(proxy_socket);
    }

    void Zipper::request_last_messages(Message &req) {
        // obtain lock
        lock_guard<mutex> lock(mu_);

        NodeId failed_proxy = req.sender_id;
        cout << "[zipper] received report for proxy_id " << failed_proxy << endl;

        // create set if this is the first reporting
        if (blocked_for_reconfiguration_.find(failed_proxy) == blocked_for_reconfiguration_.end()) {
            blocked_for_reconfiguration_[failed_proxy] = set<NodeId>();
        } else {
            return; // return if we already serviced this reconfiguration
        }

        Message report_req;
        report_req.type = ZIP_REQUEST;
        report_req.shard_id = shard();
        report_req.sender_id = failed_proxy;

        Message report_resp;
        unordered_map<SequenceNumber, int> sequence_counts;  // seq -> how many servers have it
        unordered_map<NodeId, SequenceNumber> server_sequences;  // server -> its last seq

        for (NodeId i = 0; i < config_.servers.size(); i++) {
            const auto& [server_ip, server_port] = config_.servers[i];
            if (NetworkUtils::request_from_zipper(server_ip, server_port, report_req, report_resp, config_.timeout_ms, config_.max_retries)) {
                SequenceNumber last_sequence = report_resp.get_sequence_number();

                blocked_for_reconfiguration_[failed_proxy].insert(i);

                server_sequences[i] = last_sequence;

                // cout how many servers have at least this sequence
                for (SequenceNumber seq = 0; seq <= last_sequence; seq++) {
                    sequence_counts[seq]++;
                }
            }
        }

        // determine max recoverable number of messages
        SequenceNumber max_recoverable_seq = 0;
        for (const auto& [seq, count] : sequence_counts) {
            if (count >= config_.quorum() && seq > max_recoverable_seq) {
                max_recoverable_seq = seq;
            }
        }

        cout << "[zipper] max recoverable sequence for proxy " << failed_proxy << " is " << max_recoverable_seq << endl;

        // find server with the longest log
        NodeId longest_server = 0;
        SequenceNumber longest_seq = 0;
        for (const auto& [server_id, seq] : server_sequences) {
            if (seq > longest_seq) {
                longest_seq = seq;
                longest_server = server_id;
            }
        }

        cout << "[zipper] server with longest log for proxy " << failed_proxy << " is " << longest_server << " with " << longest_seq << endl;

        // take note to start reconfiguration later
        proxy_last_sequence_[failed_proxy] = max_recoverable_seq;
    }

    void Zipper::update_slot_estimate(Message &req) {
        // validate request
        if (req.shard_id != shard() || !config_.isValidProxy(req.sender_id)) {
            return;
        }

        // obtain lock
        lock_guard<mutex> lock(mu_);

        if (req.get_num_requests()) {
            cout << "Received request from proxy " << req.sender_id << " with " << req.get_num_requests() << " timestamp(s)" << endl;
        }

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
        next_epoch_ = epoch_startup_ + EPOCH_DURATION_MS;

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
                next_epoch_ = epoch_startup_ + EPOCH_DURATION_MS;
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
            if (blocked_for_reconfiguration_.find(proxy_id) != blocked_for_reconfiguration_.end()) continue;
            if (estimate == 0) continue;

            double interval = config_.epoch_duration_ms / estimate;
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
            proxy_sequence_numbers[p.second].push_back(next_epoch_ + p.first);
            proxy_sequence_numbers[p.second].push_back(global_seq_num_++);
        }

        // respond to all in this epoch
        for (const auto& [proxy_id, values] : proxy_sequence_numbers) {

            Message resp;
            resp.type = ZIP_RESPONSE;
            resp.shard_id = shard();
            resp.sender_id = proxy_id;
            resp.set_num_requests(static_cast<SequenceNumber>(values.size() / 2));
            resp.set_assigned_sequences(values); // {timestamp, seq_num, timestamp, seq_num, ...}

            auto& [proxy_ip, proxy_port] = config_.proxies[proxy_id];
            NetworkUtils::send_message_to_address(proxy_ip, proxy_port, resp, config_.timeout_ms, config_.max_retries);

            // share sequence numbers to all srevers too
            for (const auto& [server_ip, server_port] : config_.servers) {
                NetworkUtils::send_message_to_address(server_ip, server_port, resp, config_.timeout_ms, config_.max_retries);
            }

            std::cout << "proxy " << proxy_id << " : [";
            for (size_t i = 0; i < values.size(); ++i) {
                std::cout << values[i];
                if (i < values.size() - 1) std::cout << ", ";
            }
            std::cout << "]" << std::endl;
        }
    }
}}