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
        cout << "[zipper] handle_connection() called ";
        // read from and respond to valid request (shards match and know proxy id)
        Message req;
        if (!NetworkUtils::recv_message(proxy_socket, req)) {
            close(proxy_socket);
           cout << endl;
            cout << "[zipper] handle_connection() exitting" << endl;
            return;
        }

        if (req.type == ZIP_REQUEST) {
            cout << "ZIP REQUEST" << endl;
            update_slot_estimate(req);
            close(proxy_socket);
            cout << "[zipper] handle_connection() exitting" << endl;
            return;
        }

        if (req.type == REPORT) {
            cout << "REPORT" << endl;
            request_last_messages(req);
        }

        if (req.type == FREEZE_RESPONSE) {
            cout << "FREEZE RESPONSE" << endl;
            handle_freeze_response(req);
            close(proxy_socket);
            cout << "[zipper] handle_connection() exitting" << endl;
            return;
        }

        if (req.type == REGISTER_PROXY) {
            cout << "REGISTER PROXY" << endl;
            add_proxy(req, true);
            close(proxy_socket);
            return;
        }

        if (req.type == REGISTER_SUBSCRIBER) {
            cout << "REGISTER SUBSCRIBER" << endl;
            introduce_subscriber(req);
        }

        if (req.type == REJOIN_PROXY) {
            cout << "REJOIN PROXY" << endl;
            add_proxy(req, false);
            return;
        }

        Message resp;
        resp.type = ACK;
        NetworkUtils::send_message(proxy_socket, resp);
        close(proxy_socket);
        cout << "[zipper] handle_connection() exitting" << endl;
    }

    void Zipper::request_last_messages(Message &req) {
        cout << "[zipper] request_last_messages() called" << endl;

         // obtain lock
        mu_.lock();

        // determine if we have already serviced this proxy
        NodeId failed_proxy = req.get_failed_proxy();
        cout << "[zipper] received report for proxy_id " << failed_proxy << endl;

        if (blocked_for_reconfiguration_.find(failed_proxy) != blocked_for_reconfiguration_.end()) {
            cout << "[zipper] request_last_messages() exitting" << endl;
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
            cout << "[zipper] request_last_messages() exitting" << endl;
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
        cout << "[zipper] request_last_messages() exitting" << endl;
    }

    void Zipper::send_freeze(NodeId failed_proxy, bool first_round) {
        cout << "[zipper] send_freeze() called" << endl;

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
                cout << "[zipper] send_freeze() trying send to " << server_addr.port << endl;
                int sock = NetworkUtils::create_connector_socket();
                if (sock < 0) return;

                if (NetworkUtils::connect_to_address(sock, server_addr.ip, server_addr.port)) {
                    NetworkUtils::send_message(sock, freeze);
                }
                close(sock);
                cout << "[zipper] send_freeze() ending send to " << server_addr.port << endl;
            }).detach();
        }
        cout << "[zipper] send_freeze() exitting" << endl;
    }

    void Zipper::handle_freeze_response(const Message &msg) {
        cout << "[zipper] handle_freeze_resposne() called" << endl;

        mu_.lock();
        // return if response is outdated
        NodeId failed_proxy = msg.get_failed_proxy();
        if (blocked_for_reconfiguration_[failed_proxy] || (rounds_.find(failed_proxy) != rounds_.end() && msg.get_round() < rounds_[failed_proxy])) {
            cout << "zipper outdated freeze response" << endl;
            mu_.unlock();
            return;
        }

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
        cout << "[zipper] handle_freeze_resposne() exitting" << endl;
    }

    void Zipper::send_freeze_complete(NodeId failed_proxy, SequenceNumber last_seq) {
        cout << "[zipper] send_freeze_complete() called" << endl;
        mu_.lock();
        blocked_for_reconfiguration_[failed_proxy] = true;
        rounds_.erase(failed_proxy);
        vector<SequenceNumber> allocated_seq = proxy_allocated_sequences_[failed_proxy];
        mu_.unlock();
        cout << "[zipper] freeze complete for proxy " << failed_proxy << endl;
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
                    cout << "[zipper] send_freeze_complete() trying send to " << server_port << endl;
                    Message ack;
                    NetworkUtils::send_message_to_address(server_ip, server_port, skip, ack, config_.max_retries);
                    cout << "[zipper] send_freeze_complete() ending send to " << server_port << endl;
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
            int sock = NetworkUtils::create_connector_socket();
            if (sock < 0) return;

            if (NetworkUtils::connect_to_address(sock, server_ip, server_port)) {
                NetworkUtils::send_message(sock, freeze_complete);
            }
        }
        cout << "[zipper] send_freeze_complete() exitting" << endl;
    }

    void Zipper::update_slot_estimate(Message &req) {
        // obtain lock
        lock_guard<mutex> lock(mu_);
        cout << "[zipper] update_slot_estimate() called" << endl;

        // validate request
        if (req.shard_id != shard() || !config_.isValidProxy(req.sender_id)) {
            cout << "[zipper] update_slot_estimate() exitting" << endl;
            cout << "unknown prpoxy" << endl;
            return;
        }

        if (req.get_num_requests()) {
            cout << "Received request from proxy " << req.sender_id << " with " << req.get_num_requests() << " timestamp(s)" << endl;
        }

        // take note of number fo requests
        proxy_estimates_[req.sender_id] = req.get_num_requests();
        cout << "[zipper] update_slot_estimate() exitting" << endl;
    }

    void Zipper::shutdown() {
        BaseNode::shutdown();
        cout << "Zipper shutting down" << endl;
    }

    Zipper::~Zipper() {
        is_running_ = false;
        epoch_running_ = false;

        {
        std::lock_guard<std::mutex> lock(epoch_cv_mutex_);
        epoch_cv_.notify_all();
        }

        if (epoch_thread_.joinable()) {
            cout << "[zipper] joining epoch thread" << endl;
            epoch_thread_.join();
            cout << "[zipper] joined epoch thread" << endl;
        }

        shutdown();
    }

    void Zipper::add_proxy(const Message& msg, bool is_new) {
        cout << "[zipper] add_proxy() called" << endl;
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
            cout << "[zipper] add_proxy() sending to " << server_port << endl;
            futures.push_back(std::async(std::launch::async, [=]() {
                int sock = NetworkUtils::create_connector_socket();
                if (sock < 0) return false;

                if (NetworkUtils::connect_to_address(sock, server_ip, server_port)) {
                    if (NetworkUtils::send_message(sock, join)) return true;
                }
                return false;
            }));
        }

        size_t successful_sends = 0;
        for (auto& f : futures) {
            if (f.get()) successful_sends++;
        }

        // add address to queue so zipper may respond at epoch boundary
        if (successful_sends >= quorum()) joining_proxies_.push_back({ip, port});
        cout << "[zipper] add_proxy() exitting" << endl;
    }

    void Zipper::epoch_timer() {
        cout << "[zipper] epoch_timer() exiting" << endl;
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
                //cout << "[zipper] a" << endl;
                allocate_slots();
                //cout << "[zipper] b" << endl;
                std::this_thread::sleep_for(10ms);
                //cout << "[zipper] c" << endl;
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

            //std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
            //cout << "[zipper] d" << endl;
            std::unique_lock<mutex> lock(epoch_cv_mutex_);
            //cout << "[zipper] e" << endl;
            if (epoch_cv_.wait_for(lock, std::chrono::milliseconds(sleep_duration), [this]() {
                //cout << "[zipper " << id() << "] checking condition" << endl;
                return !epoch_running_.load();
            })) {
                // Predicate returned true, exit immediately
                cout << "[zipper " << id() << "] condition met, exiting" << endl;
                break;
            }
        }
        cout << "[zipper] epoch_timer() exiting" << endl;
    }

    void Zipper::introduce_proxies() {
        cout << "[zipper] introduce_proxies() called" << endl;
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
                cout << "[zipper] introduce_proxies() trying send to " << port << endl;
                int sock = NetworkUtils::create_connector_socket();
                if (sock < 0) return;

                if (NetworkUtils::connect_to_address(sock, ip, port)) {
                    NetworkUtils::send_message(sock, intro);
                    cout << "[zipper] introduce_proxies() ending send to " << port << endl;
                } else {
                    cout << "[zipper] introduce_proxies() ending send to " << port << endl;
                }
                close(sock);
                cout << "[zipper] introduce_proxies() called" << endl;
            }));
        }

        // wait for completion
        for (auto& f : futures) {
            f.wait();
        }

        cout << "[zipper] introduce_proxies() exitting" << endl;
    }

    void Zipper::introduce_subscriber(const Message& msg) {
        cout << "[zipper] introduce_subscriber() called" << endl;
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
            //cout << "[zipper] introduce_subscriber() trying send to " << server_ip << endl;
            futures.push_back(std::async(std::launch::async, [=]() {
                int sock = NetworkUtils::create_connector_socket();
                if (sock < 0) return false;

                if (NetworkUtils::connect_to_address(sock, server_ip, server_port)) {
                    if (NetworkUtils::send_message(sock, join)) return true;
                }
                return false;
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
        config_.subscribers.push_back({ip, port});
        mu_.unlock();

        // send response to subscriber (remove ack)
        Message intro;
        intro.type = INCLUDE_SUBSCRIBER;
        intro.shard_id = shard();

        int sock = NetworkUtils::create_connector_socket();
        if (sock < 0) return;

        if (NetworkUtils::connect_to_address(sock, ip, port)) {
            NetworkUtils::send_message(sock, intro);
        }
        cout << "[zipper] introduce_subscriber() exitting" << endl;
    }

    void Zipper::allocate_slots() {

        // obtain lock
        mu_.lock();
        cout << "[zipper] allocate_slots() called" << endl;
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
        cout << "[zipper] allocate_slots() exitting" << endl;
    }

    void Zipper::deliver_slot_allocation(NodeId proxy_id, const vector<SequenceNumber>& values) {
        cout << "[zipper] deliver_slot_allocation() called" << endl;
        Message resp;
        resp.type = ZIP_RESPONSE;
        resp.shard_id = shard();
        resp.sender_id = proxy_id;
        resp.set_num_requests(static_cast<SequenceNumber>(values.size() / 2));
        resp.set_assigned_sequences(values); // {timestamp, seq_num, timestamp, seq_num, ...}

        auto& [proxy_ip, proxy_port] = config_.proxies[proxy_id];
        int sock = NetworkUtils::create_connector_socket();
        if (sock < 0) return;

        if (NetworkUtils::connect_to_address(sock, proxy_ip, proxy_port)) {
            NetworkUtils::send_message(sock, resp);
        }

        // share sequence numbers to all srevers too
        for (const auto& [server_ip, server_port] : config_.servers) {
                int sock = NetworkUtils::create_connector_socket();
                if (sock < 0) return;

                if (NetworkUtils::connect_to_address(sock, server_ip, server_port)) {
                    NetworkUtils::send_message(sock, resp);
                }
        }

        std::cout << "proxy " << proxy_id << " : [";
        for (size_t i = 0; i < values.size(); ++i) {
            std::cout << values[i];
            if (i < values.size() - 1) std::cout << ", ";
        }
        std::cout << "]" << std::endl;
        cout << "[zipper] deliver_slot_allocation() exitting" << endl;
    }
}}