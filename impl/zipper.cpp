#include "zipper.h"
#include "network_utils.h"
#include <sys/socket.h>     // accept()
#include <netinet/in.h>     // sockaddr_in
#include <unistd.h>
#include <iostream>
#include <algorithm>
#include <unordered_map>

using namespace ziplog::api;
using namespace std::literals;

namespace ziplog {
namespace impl {

    Zipper::Zipper(const ziplogConfig& cfg) {
        config = cfg;
        shard_id = 0;
        auto [ip, p] = cfg.zipper;
        ipAddress = ip;
        port = p;
        isRunning = false;
        zipper_sock = -1;

        // global sequencing state
        numProxies = cfg.proxies.size();
        globalSeqNum = 0;

        running_thread = std::thread(&Zipper::run, this);
        epoch_thread = std::thread(&Zipper::epochTimer, this);

    }

    void Zipper::run() {
        if (isRunning) {
            std::cerr << "Zipper already running" << std::endl;
            return;
        }
        isRunning = true;

        // create listening socket
        zipper_sock = NetworkUtils::createListeningSocket(ipAddress, port, true);
        if (zipper_sock < 0) {
            std::cerr << "Zipper failed to create server socket" << std::endl;
            return;
        }

        // handle connections
        while (isRunning) {
            struct sockaddr_in proxy_addr;
            socklen_t proxy_len = sizeof(proxy_addr);

            // accept connection
            int proxy_socket = accept(zipper_sock, (struct sockaddr*)&proxy_addr, &proxy_len);
            if (proxy_socket < 0) {
                if (isRunning) {
                    std::cerr << "Failed to accept connection" << std::endl;
                    continue;
                } else {
                    break;
                }
            }

            // handle request in thread
            std::thread proxy_thread(&Zipper::handleProxy, this, proxy_socket);
            proxy_thread.detach();
        }
    }

    void Zipper::handleProxy(int proxy_socket) {
        // read from and respond to valid request (shards match and know proxy id)
        message req;
        if (!NetworkUtils::recvMessage(proxy_socket, req)) {
            close(proxy_socket);
            return;
        }

        std::cout << "Received request from proxy " << req.sender_id
                  << " with " << req.ordering_values.size() << " timestamp(s)" << std::endl;

        if (req.type == ZIP_REQUEST && req.shard_id == static_cast<uint32_t>(shard_id) && req.sender_id < static_cast<uint64_t>(numProxies)) {
            // obtain lock
            std::lock_guard<std::mutex> lock(counter_mutex);

            // take note of batch request
            pending_requests.push_back({req.sender_id, req.seq_or_count, proxy_socket});

            // timer will handle the response
        } else {
            close(proxy_socket);    // signifies completion of operation
        }
    }

    void Zipper::shutdown() {
        isRunning = false;
        if (zipper_sock >= 0) {
            ::shutdown(zipper_sock, SHUT_RDWR);
            close(zipper_sock);
            zipper_sock = -1;
        }
        std::cout << "Zipper shutting down" << std::endl;
    }

    Zipper::~Zipper() {
        shutdown();
        if (running_thread.joinable()) {
            running_thread.join();  // wait for thread to finish
        }

        if (epoch_thread.joinable()) {
            epoch_thread.join();
        }
    }

    void Zipper::epochTimer() {
        epoch_startup = std::chrono::system_clock::now();

        while (isRunning) {
            auto now = std::chrono::system_clock::now();
            auto elapsed = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now - epoch_startup).count());

            const uint64_t allocation_time = (EPOCH_DURATION_MS * 3) / 4;

            if (elapsed >= allocation_time and elapsed < (allocation_time + 10)) {
                // allocate slots at 3/4 point
                Zipper::allocateSlots();
                std::this_thread::sleep_for(10ms);
            }

            if (elapsed >= EPOCH_DURATION_MS) {
                // restart timer
                epoch_startup = std::chrono::system_clock::now();
            }

            std::this_thread::sleep_for(5ms);
        }
    }

    void Zipper::allocateSlots() {
        // obtain lock
        std::lock_guard<std::mutex> lock(counter_mutex);

        std::cout << "Allocating slots for " << pending_requests.size() << " requests" << std::endl;

        if (pending_requests.empty()) return;

        std::unordered_map<uint32_t, int> proxy_sockets;    // proxy_id > socket
        vector<pair<uint32_t, uint64_t>> proxy_slot_count;       // {proxy_id, num_slots}

        for (const auto& batch : pending_requests) {
            // store connector socket and number of requests
            proxy_sockets[batch.proxy_id] = batch.proxy_socket;
            proxy_slot_count.push_back({batch.proxy_id, batch.num_slots});
        }

        // sort based on number of requested slots (desc order)
        std::sort(proxy_slot_count.begin(), proxy_slot_count.end(), [](const auto& a, const auto& b) { return a.second > b.second; });
        std::cout << "sorted" << std::endl;

        // get max slots
        uint32_t max_slots_proxy = proxy_slot_count[0].first;
        uint64_t max_slots = proxy_slot_count[0].second;

        // sequence tracking
        std::unordered_map<uint32_t, vector<uint64_t>> proxy_sequence_numbers;

        // initialize errors
        double init = static_cast<double>(max_slots) / proxy_slot_count.size();
        vector<double> errors(proxy_slot_count.size() - 1, init);

        // interleave slots
        for (uint64_t slot = 0; slot < max_slots; slot++) {
            std::cout << "b" << std::endl;
            // yield to max proxy
            proxy_sequence_numbers[max_slots_proxy].push_back(globalSeqNum++);

            for (size_t i = 1; i < proxy_slot_count.size(); i++) {
                std::cout << "a" << std::endl;
                uint32_t slots_proxy = proxy_slot_count[i].first;
                uint64_t slots = proxy_slot_count[i].second;

                errors[i - 1] -= slots;
                if (errors[i - 1] < 0) {
                    proxy_sequence_numbers[slots_proxy].push_back(globalSeqNum++);
                    errors[i - 1] += max_slots; // ?
                }
            }
        }

        std::cout << "Allocated slots ------------------------------------------------------" << std::endl;

        // respond to all in this epoch
        for (const auto& [proxy_id, values] : proxy_sequence_numbers) {
            // get proxy socket
            int recipient = proxy_sockets[proxy_id];

            message resp;
            resp.type = ZIP_RESPONSE;
            resp.shard_id = shard_id;
            resp.seq_or_count = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(epoch_startup.time_since_epoch()).count());
            resp.set_num_requests(static_cast<uint64_t>(values.size()));
            resp.set_assigned_sequences(values);

            NetworkUtils::sendMessage(recipient, resp);
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
        pending_requests.clear();
    }
}}