#pragma once
#include "../api/address.h"
#include "../api/types.h"
#include "../api/network_utils.h"
#include <unordered_map>
#include <queue>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <memory>
#include <atomic>

using namespace ziplog::api;

namespace ziplog::impl
{

    class ServerReplicator
    {
    public:
        ServerReplicator(const std::vector<Address> &servers, size_t quorum)
            : servers_(servers), quorum_(quorum) {}

        ~ServerReplicator() { shutdown(); }

        void start()
        {
            for (size_t i = 0; i < servers_.size(); i++)
            {
                auto worker = std::make_unique<ServerWorker>();
                server_workers_[i] = std::move(worker);
                server_workers_[i]->thread = std::thread(&ServerReplicator::worker_loop, this, i);
            }
        }

        void shutdown()
        {
            std::lock_guard<std::mutex> lock(mu_);
            for (auto &[idx, worker] : server_workers_)
            {
                {
                    std::lock_guard<std::mutex> qlock(worker->queue_mu);
                    worker->shutdown = true;
                }
                worker->cv.notify_one();
            }
            for (auto &[idx, worker] : server_workers_)
            {
                if (worker->thread.joinable())
                    worker->thread.join();
            }
        }

        // blocks until f+1 acks received
        bool replicate(Message &msg)
        {
            if (msg.type == APPEND)
            {
                // cout << "[proxy] replicate called at " << std::to_string(now()) << endl;
            }
            auto state = std::make_shared<ReplicationState>();
            state->seq = msg.get_sequence_number();

            auto t0 = high_resolution_clock::now();

            {
                std::lock_guard<std::mutex> lock(mu_);
                for (auto &[idx, worker] : server_workers_)
                {
                    {
                        std::lock_guard<std::mutex> qlock(worker->queue_mu);
                        worker->pending_queue.push({msg, state});
                    }
                    worker->cv.notify_one();
                }
            }

            auto t1 = high_resolution_clock::now(); // work enqued

            if (msg.type == APPEND)
            {
                // cout << "[proxy] replicate placed work at " << std::to_string(now()) << endl;
            }

            // wait for quorum acks
            std::unique_lock<std::mutex> lock(state->mu);
            state->cv.wait(lock, [&]()
                           { return state->ack_count >= static_cast<int>(quorum_); });

            auto t2 = high_resolution_clock::now(); // quorum reached

            if (msg.type == APPEND)
            {
                auto enqueue = duration_cast<microseconds>(t1 - t0).count();
                auto wait = duration_cast<microseconds>(t2 - t1).count();
                cout << "[replicate()] enqueue=" << enqueue << "µs"
                     << " quorum_wait=" << wait << "µs"
                     << " total=" << duration_cast<microseconds>(t2 - t0).count() << "µs\n";
            }
            return true;
        }

    private:
        struct ReplicationState
        {
            std::mutex mu;
            std::condition_variable cv;
            std::atomic<int> ack_count{0};
            SequenceNumber seq;
        };

        struct WorkItem
        {
            Message msg;
            std::shared_ptr<ReplicationState> state;
        };

        struct ServerWorker
        {
            std::thread thread;
            int socket{-1};
            std::queue<WorkItem> pending_queue;
            std::mutex queue_mu;
            std::condition_variable cv;
            std::atomic<bool> shutdown{false};
        };

        void worker_loop(int server_idx)
        {
            ServerWorker &worker = *server_workers_[server_idx];
            const Address &server = servers_[server_idx];

            // keep retrying until connected or shutdown
            while (worker.socket < 0 && !worker.shutdown)
            {
                worker.socket = NetworkUtils::connect_to_address_persistent(server.ip, server.port);
                if (worker.socket < 0)
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }

            while (!worker.shutdown)
            {
                WorkItem item;
                auto t0 = high_resolution_clock::now();

                {
                    std::unique_lock<std::mutex> lock(worker.queue_mu);
                    worker.cv.wait(lock, [&]()
                                   { return !worker.pending_queue.empty() || worker.shutdown.load(); });
                    if (worker.shutdown)
                        break;

                    item = worker.pending_queue.front();
                    worker.pending_queue.pop();
                }

                auto t1 = high_resolution_clock::now(); // deque

                if (item.msg.type == APPEND)
                {
                    // cout << "[proxy worker] sending at " << std::to_string(now()) << endl;
                }

                // send and wait for ack on persistent connection
                bool ok = NetworkUtils::send_message(worker.socket, item.msg);

                auto t2 = high_resolution_clock::now(); // sent message

                if (ok)
                {
                    Message ack;
                    ok = NetworkUtils::recv_message(worker.socket, ack);
                }

                auto t3 = high_resolution_clock::now(); // recv ack

                if (!ok)
                {
                    worker.shutdown = true;
                    break;
                }

                if (item.msg.type == APPEND)
                {
                    // cout << "[proxy worker] recv ack " << std::to_string(now()) << endl;
                }

                // signal quorum waiter
                if (item.state)
                {
                    std::lock_guard<std::mutex> lock(item.state->mu);
                    item.state->ack_count++;
                    item.state->cv.notify_one();
                }

                auto t4 = high_resolution_clock::now(); // quorum signaled

                if (item.msg.type == APPEND)
                {
                    auto cv_wait = duration_cast<microseconds>(t1 - t0).count();
                    auto send = duration_cast<microseconds>(t2 - t1).count();
                    auto recv_ack = duration_cast<microseconds>(t3 - t2).count();
                    auto signal = duration_cast<microseconds>(t4 - t3).count();
                    auto total = duration_cast<microseconds>(t4 - t1).count();

                    cout << "[replicator server=" << server_idx << "]"
                         << " cv_wait=" << cv_wait << "µs"
                         << " send=" << send << "µs"
                         << " recv_ack=" << recv_ack << "µs"
                         << " signal=" << signal << "µs"
                         << " total=" << total << "µs\n";
                }
            }
            close(worker.socket);
        }

        std::vector<Address> servers_;
        size_t quorum_;
        std::mutex mu_;
        std::unordered_map<int, std::unique_ptr<ServerWorker>> server_workers_;
    };

} // namespace ziplog::impl