#include "zipper.h"
#include <algorithm>

using namespace ziplog::api;
using std::future;

namespace ziplog {
namespace impl {

    Zipper::Zipper(const ZipperConfig& cfg) :
        BaseNode<ZipperConfig>(cfg)
    {
        // global sequencing state
        global_seq_num_ = 1;

        // initialize proxy estimate tracker
        for (NodeId i = 0; i < cfg.proxies.size(); i++) {
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

        if (req.type == FREEZE_RESPONSE) {
            handle_freeze_response(req);
            return;
        }

        if (req.type == REGISTER_PROXY) {
            add_proxy(req, true);
        }

        if (req.type == REGISTER_SUBSCRIBER) {
            introduce_subscriber(req);
        }

        if (req.type == REJOIN_PROXY) {
            add_proxy(req, false);
        }

        Message resp;
        resp.type = ACK;
        NetworkUtils::send_message(proxy_socket, resp);
        close(proxy_socket);
    }

    void Zipper::request_last_messages(Message &req) {
         // obtain lock
        mu_.lock();

        // determine if we have already serviced this proxy
        NodeId failed_proxy = req.get_failed_proxy();
        cout << "[zipper] received report for proxy_id " << failed_proxy << endl;

        if (blocked_for_reconfiguration_.find(failed_proxy) != blocked_for_reconfiguration_.end()) {
            mu_.unlock();
            return;
        }

        // create set if this is the first reporting
        if (reported_proxies_.find(failed_proxy) == reported_proxies_.end()) {
            reported_proxies_[failed_proxy] = set<NodeId>();
        }

        // add sender to set of reporters
        reported_proxies_[failed_proxy].insert(req.sender_id);

        // return if we have not reached quorum on this reportin yet
        if (reported_proxies_[failed_proxy].size() < quorum()) {
            mu_.unlock();
            return;
        }

        // begin freezing
        bool new_round = false;
        if (blocked_for_reconfiguration_.find(failed_proxy) == blocked_for_reconfiguration_.end()) {
            blocked_for_reconfiguration_[failed_proxy] = false;
            new_round = true;
        }

        mu_.unlock();

        if (new_round) {
            send_freeze(failed_proxy, true);
        }
    }

    void Zipper::send_freeze(NodeId failed_proxy, bool first_round) {
        mu_.lock();
        int round = 1;
        if (!first_round) {
            round = rounds_[failed_proxy] + 1;
        }

        rounds_[failed_proxy] = round;
        rounds_responders_[failed_proxy] = set<NodeId>();
        proxy_last_sequence_[failed_proxy] = set<SequenceNumber>();

        vector<Address> servers_copy = config_.servers;
        mu_.unlock();

        Message freeze;
        freeze.type = FREEZE;
        freeze.shard_id = shard();
        freeze.set_failed_proxy(failed_proxy);
        freeze.set_round(round);

        for (const auto& server_addr : servers_copy) {
            thread([server_addr, freeze]() {
                int sock = NetworkUtils::create_connector_socket();
                if (sock < 0) return;

                if (NetworkUtils::connect_to_address(sock, server_addr.ip, server_addr.port)) {
                    NetworkUtils::send_message(sock, freeze);
                }
                close(sock);
            }).detach();
        }
        cout << "[zipper] Broadcasted FREEZE for proxy " << failed_proxy << " round " << round << endl;
    }

    void Zipper::handle_freeze_response(const Message &msg) {
        mu_.lock();
        // return if response is outdated
        NodeId failed_proxy = msg.get_failed_proxy();
        if (rounds_.find(failed_proxy) == rounds_.end() || msg.get_round() == rounds_[failed_proxy]) return;

        // add info to tracker
        rounds_responders_[failed_proxy].insert(msg.sender_id);
        proxy_last_sequence_[failed_proxy].insert(msg.get_sequence_number());

        // see if we can move to next round/complete freeze
        bool round_complete = false;
        bool freeze_complete = false;
        int last_seq = -1;

        if (rounds_responders_[failed_proxy].size() == quorum()) {
            round_complete = true;
            if (proxy_last_sequence_[failed_proxy].size() == 1) {
                // bcast freeze complete
                freeze_complete = true;
                last_seq = *proxy_last_sequence_[failed_proxy].begin();
            }
        }
        mu_.unlock();

        if (round_complete) {
            if (freeze_complete) {
                // bcast freeze complete
                send_freeze_complete(failed_proxy, last_seq);
            } else {
                // start new round
                send_freeze(failed_proxy, false);
            }
        }
    }

    void Zipper::send_freeze_complete(NodeId failed_proxy, SequenceNumber last_seq) {
        mu_.lock();
        blocked_for_reconfiguration_[failed_proxy] = true;
        vector<SequenceNumber> allocated_seq = proxy_allocated_sequences_[failed_proxy];
        mu_.unlock();

        for (auto& seq : allocated_seq) {
            if (seq > last_seq) {
                cout << "[zipper] sending skip for seq " << seq << endl;
                // sequence was allocated but never used - send SKIP
                Message skip;
                skip.type = SKIP;
                skip.shard_id = shard();
                skip.sender_id = failed_proxy;
                skip.set_sequence_number(seq);

                // bcast SKIP to all servers
                for (NodeId i = 0; i < config_.servers.size(); i++) {
                    const auto& [server_ip, server_port] = config_.servers[i];
                    Message ack;
                    NetworkUtils::send_message_to_address(server_ip, server_port, skip, ack, config_.max_retries);
                }
            }
        }

        Message freeze_complete;
        freeze_complete.type = FREEZE_COMPLETE;
        freeze_complete.shard_id = shard();
        freeze_complete.set_failed_proxy(failed_proxy);
        freeze_complete.set_sequence_number(last_seq);

        for (NodeId i = 0; i < config_.servers.size(); i++) {
            const auto& [server_ip, server_port] = config_.servers[i];
            Message ack;
            NetworkUtils::send_message_to_address(server_ip, server_port, freeze_complete, ack, config_.max_retries);
        }
    }

    void Zipper::update_slot_estimate(Message &req) {
        // validate request
        cout << "[zipper] recv proxy request" << endl;
        if (req.shard_id != shard() || !config_.isValidProxy(req.sender_id)) {
            cout << "unknown prpoxy" << endl;
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

    void Zipper::add_proxy(const Message& msg, bool is_new) {
        // deserialize address
        string addr_info(msg.data.begin(), msg.data.end());
        size_t colon_pos = addr_info.find(':');
        string ip = addr_info.substr(0, colon_pos);
        int port = std::stoi(addr_info.substr(colon_pos + 1));

        // clear tracking data for rejoining proxy
        if (!is_new) {
            mu_.lock();
            NodeId proxy_id = msg.sender_id;
            blocked_for_reconfiguration_.erase(proxy_id);
            reported_proxies_.erase(proxy_id);
            mu_.unlock();
        }

        // broadcast new proxy to servers
        Message join;
        join.type = INCLUDE_PROXY;
        join.shard_id = shard();
        join.sender_id = msg.sender_id;
        join.data = Command(addr_info.begin(), addr_info.end());

        vector<future<bool>> futures;
        for (size_t i = 0; i < num_servers(); i++) {
            auto [server_ip, server_port] = config_.servers[i];

            futures.push_back(std::async(std::launch::async, [=]() {
                Message ack;
                if (!NetworkUtils::send_message_to_address(server_ip, server_port, join, ack, config_.max_retries)) {
                    std::cerr << "[zipper] failed to recv ack for proxy " << msg.sender_id << " join from server " << i << std::endl;
                    return false;
                }
                return true;
            }));
        }

        size_t successful_sends = 0;
        for (auto& f : futures) {
            if (f.get()) successful_sends++;
        }

        // add address to queue so zipper may respond at epoch boundary
        if (successful_sends >= quorum()) joining_proxies_.push_back({ip, port});
    }

    void Zipper::epoch_timer() {
        epoch_running_ = true;
        epoch_startup_ = now_ms();
        next_epoch_ = epoch_startup_ + config_.epoch_duration_ms;
        const Timestamp allocation_time = (config_.epoch_duration_ms * 3) / 4;
        const Timestamp allocation_buffer = std::max(static_cast<Timestamp>(1), config_.epoch_duration_ms / 100);  // 1% of epoch, min 1ms
        const Timestamp sleep_duration = std::max(static_cast<Timestamp>(1), config_.epoch_duration_ms / 200);

        while (epoch_running_) {
            Timestamp now = now_ms();
            Timestamp elapsed = now - epoch_startup_;

            if (elapsed >= allocation_time && elapsed < (allocation_time + allocation_buffer)) {
                // allocate slots at 3/4 point
                allocate_slots();
                std::this_thread::sleep_for(10ms);
            }

            if (elapsed >= config_.epoch_duration_ms) {
                // let in waiting proxies at epoch boundary
                std::thread([this]() {
                    introduce_proxies();
                }).detach();
                // restart timer
                epoch_startup_ = now_ms();
                next_epoch_ = epoch_startup_ + config_.epoch_duration_ms;

            }

            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
        }
    }

    void Zipper::introduce_proxies() {
        // obtain lock
        mu_.lock();
        deque<pair<string, int>> proxies_to_add = joining_proxies_;
        joining_proxies_.clear();
        mu_.unlock();

        vector<future<void>> futures;
        for (const auto& [ip, port] : proxies_to_add) {
            futures.push_back(std::async(std::launch::async, [this, ip, port]() {
                mu_.lock();
                config_.proxies.push_back({ip, port});
                mu_.unlock();

                // respond to proxy
                Message intro;
                intro.type = INCLUDE_PROXY;
                intro.shard_id = shard();

                Message ack;
                NetworkUtils::send_message_to_address(ip, port, intro, ack, config_.max_retries);
            }));
        }

        // wait for completion
        for (auto& f : futures) {
            f.wait();
        }
    }

    void Zipper::introduce_subscriber(const Message& msg) {
        // deserialize address
        string addr_info(msg.data.begin(), msg.data.end());
        size_t colon_pos = addr_info.find(':');
        string ip = addr_info.substr(0, colon_pos);
        int port = std::stoi(addr_info.substr(colon_pos + 1));

        // broadcast new subscriber to servers
        Message join;
        join.type = INCLUDE_SUBSCRIBER;
        join.shard_id = shard();
        join.sender_id = msg.sender_id;
        join.data = Command(addr_info.begin(), addr_info.end());

        vector<future<bool>> futures;
        for (size_t i = 0; i < num_servers(); i++) {
            auto [server_ip, server_port] = config_.servers[i];

            futures.push_back(std::async(std::launch::async, [=]() {
                Message ack;
                if (!NetworkUtils::send_message_to_address(server_ip, server_port, join, ack, config_.max_retries)) {
                    std::cerr << "[zipper] failed to recv ack for subscriber " << msg.sender_id << " join from server " << i << std::endl;
                    return false;
                }
                return true;
            }));
        }

        size_t successful_sends = 0;
        for (auto& f : futures) {
            if (f.get()) successful_sends++;
        }

        // add subscriber to config and
        if (successful_sends < quorum()) return;

        // add subscriber to config
        mu_.lock();
        config_.subscribers->push_back({ip, port});
        mu_.unlock();

        // send response to subscriber
        Message intro;
        intro.type = INCLUDE_SUBSCRIBER;
        intro.shard_id = shard();

        Message ack;
        NetworkUtils::send_message_to_address(ip, port, intro, ack, config_.max_retries);
    }

    void Zipper::allocate_slots() {
        // obtain lock
        mu_.lock();

        cout << "[zipper] ---------------------------PROXY ESTIMATES" << endl;
        for (auto& [proxy_id, est] : proxy_estimates_) {
            if (reported_proxies_.find(proxy_id) != reported_proxies_.end()) continue;
            cout << "proxy " << proxy_id << ": " << est << endl;
        }

        vector<pair<double, NodeId>> timestamps;    // vector or {timestamp, proxy_id}

        for (const auto& [proxy_id, estimate] : proxy_estimates_) {
            if (reported_proxies_.find(proxy_id) != reported_proxies_.end()) continue;
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
            SequenceNumber seq_num = global_seq_num_++;
            proxy_sequence_numbers[p.second].push_back(next_epoch_ + p.first);
            proxy_sequence_numbers[p.second].push_back(seq_num);
            proxy_allocated_sequences_[p.second].push_back(seq_num);
        }

        mu_.unlock();

        // respond to all in this epoch using threads
        for (const auto& [proxy_id, values] : proxy_sequence_numbers) {
            thread t([this, proxy_id, values]() {
                deliver_slot_allocation(proxy_id, values);
            });
            t.detach();
        }
    }

    void Zipper::deliver_slot_allocation(NodeId proxy_id, const vector<SequenceNumber>& values) {
        Message resp;
        resp.type = ZIP_RESPONSE;
        resp.shard_id = shard();
        resp.sender_id = proxy_id;
        resp.set_num_requests(static_cast<SequenceNumber>(values.size() / 2));
        resp.set_assigned_sequences(values); // {timestamp, seq_num, timestamp, seq_num, ...}

        Message ack;
        auto& [proxy_ip, proxy_port] = config_.proxies[proxy_id];

        NetworkUtils::send_message_to_address(proxy_ip, proxy_port, resp, ack, config_.max_retries);

        // share sequence numbers to all srevers too
        for (const auto& [server_ip, server_port] : config_.servers) {
            NetworkUtils::send_message_to_address(server_ip, server_port, resp, ack, config_.max_retries);
        }

        std::cout << "proxy " << proxy_id << " : [";
        for (size_t i = 0; i < values.size(); ++i) {
            std::cout << values[i];
            if (i < values.size() - 1) std::cout << ", ";
        }
        std::cout << "]" << std::endl;
    }
}}