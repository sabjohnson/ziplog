#include "proxy.h"
#include "network_utils.h"
#include <sys/socket.h>     // socket(), connect(), send(), recv()
#include <netinet/in.h>     // sockaddr_in, AF_INET
#include <arpa/inet.h>      // inet_pton(), htons(), ntohs()
#include <unistd.h>         // close()
#include <cstring>          // memset()

using namespace ziplog::api;

namespace ziplog {
namespace impl {

    Proxy::Proxy(NodeId proxy_id, const ZiplogConfig &cfg) {
        // validate node id
        validate_node_id(proxy_id, cfg.num_proxies(), "Proxy");

        // set value of members (we already know ip addr is in our valid range based on parsed config)
        config_ = cfg;
        shard_id_ = 0;
        id_ = proxy_id;
        auto [ip, p] = cfg.proxies[proxy_id];
        ip_address_ = ip;
        port_ = p;
        is_running_ = true;

        batch_size_ = 3;
        // epoch tracking
        //epoch_thread = thread(&Proxy::epoch_timer(), this);
    }

    bool Proxy::append(const Command &data) {
        // get current timestamp
        Timestamp now = now_ms();

        // add timestamp and data to batch
        batch_times_.push_back(now);
        batch_values_.push_back(data);

        if (batch_times_.size() < static_cast<size_t>(batch_size_)) {
            return true;
        }

        // build request to zipper
        Message zip_req;
        zip_req.type = ZIP_REQUEST;
        zip_req.shard_id = shard_id_;
        zip_req.sender_id = id_;
        zip_req.set_num_requests(batch_size_);
        zip_req.set_timestamps(batch_times_);

        Message zip_resp;
        if (!NetworkUtils::request_from_zipper(config_.zipper.first, config_.zipper.second, zip_req, zip_resp, config_.timeout_ms, config_.max_retries)) {
            return false;
        }

        if (zip_resp.get_assigned_sequences().empty()) {
            std::cerr << "Zipper returned no sequence numbers" << std::endl;
            return false;
        }

        batch_times_.clear();
        batch_values_.clear();

        // create msg to be broadcasted
        Message msg;
        msg.type = APPEND;
        msg.shard_id = shard_id_;
        msg.sender_id = id_;
        msg.seq_or_count = zip_resp.get_assigned_sequences()[0];
        msg.data = data;
        
        // attempt to replicate on f + 1 storage servers
        size_t successful_sends = 0;
        for (size_t i = 0; i < config_.num_servers() && successful_sends < config_.quorum(); i++) {
            auto [server_ip, server_port] = config_.servers[i];

            if (NetworkUtils::send_message_to_address(server_ip, server_port, msg, config_.timeout_ms, config_.max_retries)) {
                successful_sends++;
                std::cout << "Received ACK from server " << i << std::endl;
            }
        }
        return successful_sends == config_.quorum();
    }
    
    void Proxy::shutdown() {
        is_running_ = false;
    }

    Proxy::~Proxy() {
        shutdown();
    }
}}
