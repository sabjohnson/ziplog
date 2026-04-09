#pragma once
#include "../api/types.h"
#include "network_utils.h"
#include "connection_pool.h"
#include <unordered_map>
#include <queue>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <iostream>

using namespace ziplog::api;

namespace ziplog::impl
{

    // One persistent thread per subscriber. Fire-and-forget broadcast
    // (server does not wait for acks from subscribers synchronously).
    class SubscriberBroadcaster
    {
    public:
        explicit SubscriberBroadcaster(ConnectionPool &pool) : pool_(pool) {}

        ~SubscriberBroadcaster() { shutdown(); }

        void start(const std::vector<Address> &subscribers)
        {
            for (size_t i = 0; i < subscribers.size(); i++)
            {
                add_subscriber(i, subscribers[i]);
            }
        }

        void add_subscriber(size_t idx, const Address &addr)
        {
            auto worker = std::make_unique<SubscriberWorker>();
            worker->address = addr;
            {
                std::lock_guard<std::mutex> lock(mu_);
                workers_[idx] = std::move(worker);
            }
            workers_[idx]->thread = std::thread(&SubscriberBroadcaster::worker_loop, this, idx);
        }

        void broadcast(const Message &msg)
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (msg.type == APPEND)
            {
                // cout << "[server *] broadcast called at " << std::to_string(now()) << endl;
            }
            for (auto &[idx, worker] : workers_)
            {
                {
                    std::lock_guard<std::mutex> qlock(worker->queue_mu);
                    worker->queue.push(msg);
                }
                worker->cv.notify_one();
            }
            if (msg.type == APPEND)
            {
                // cout << "[server *] broadcast placed work at " << std::to_string(now()) << endl;
            }
        }

        void shutdown()
        {
            std::lock_guard<std::mutex> lock(mu_);
            for (auto &[idx, worker] : workers_)
            {
                {
                    std::lock_guard<std::mutex> qlock(worker->queue_mu);
                    worker->shutdown = true;
                }
                worker->cv.notify_one();
            }
            for (auto &[idx, worker] : workers_)
            {
                if (worker->thread.joinable())
                    worker->thread.join();
            }
        }

    private:
        struct SubscriberWorker
        {
            Address address;
            std::thread thread;
            std::queue<Message> queue;
            std::mutex queue_mu;
            std::condition_variable cv;
            std::atomic<bool> shutdown{false};
        };

        void worker_loop(size_t idx)
        {
            SubscriberWorker &worker = *workers_[idx];

            while (!worker.shutdown)
            {
                Message msg;
                {
                    std::unique_lock<std::mutex> lock(worker.queue_mu);
                    worker.cv.wait(lock, [&]()
                                   { return !worker.queue.empty() || worker.shutdown.load(); });
                    if (worker.shutdown)
                        break;
                    msg = worker.queue.front();
                    worker.queue.pop();
                }

                int sock = pool_.get_connection(worker.address);
                if (sock < 0)
                    break;

                auto start = high_resolution_clock::now();

                if (msg.type == APPEND)
                {
                    // cout << "[server] broadcaster sending at " << std::to_string(now()) << endl;
                }

                if (NetworkUtils::send_message(sock, msg))
                {
                    Message ack;
                    NetworkUtils::recv_message(sock, ack);
                }
                auto end = high_resolution_clock::now();
                
                auto dur = duration_cast<microseconds>(end - start);
                cout << "Server broadcast latency: " << dur.count() << " us\n";
            }
        }

        ConnectionPool &pool_;
        std::mutex mu_;
        std::unordered_map<size_t, std::unique_ptr<SubscriberWorker>> workers_;
    };

} // namespace ziplog::impl