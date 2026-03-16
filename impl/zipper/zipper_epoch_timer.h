#pragma once
#include "types.h"
#include <thread>
#include <atomic>
#include <functional>
#include <chrono>

using namespace ziplog::api;
using namespace std::chrono_literals;

namespace ziplog::impl
{

    class ZipperEpochTimer
    {
    public:
        ZipperEpochTimer(EpochDurationUnit epoch_duration,
                         std::function<void()> on_allocate,
                         std::function<void()> on_epoch_end)
            : epoch_duration_(epoch_duration.count()), on_allocate_(std::move(on_allocate)), on_epoch_end_(std::move(on_epoch_end)) {}

        ~ZipperEpochTimer() { stop(); }

        void start()
        {
            {
                std::lock_guard<std::mutex> lock(work_mu_);
                running_ = true;
            }
            timer_thread_ = std::thread(&ZipperEpochTimer::run, this);
            worker_thread_ = std::thread(&ZipperEpochTimer::work_loop, this);
        }

        void stop()
        {
            cout << "stop epoch timer called" << endl;
            {
                std::lock_guard<std::mutex> lock(work_mu_);
                running_ = false;
            }
            cout << "stop epoch timer running = false" << endl;
            cv_.notify_all();
            cout << "stop epoch timer cv notified" << endl;
            if (timer_thread_.joinable())
                timer_thread_.join();
            cout << "stop epoch timer joined 1" << endl;
            if (worker_thread_.joinable())
                worker_thread_.join();
            cout << "stop epoch timer joined 2" << endl;
            cout << "stop epoch timer complete" << endl;
        }

        Timestamp next_epoch() const { return next_epoch_; }

    private:
        Timestamp epoch_duration_;
        std::atomic<Timestamp> next_epoch_{0};

        std::function<void()> on_allocate_;
        std::function<void()> on_epoch_end_;

        bool running_{false};

        std::mutex work_mu_;
        std::condition_variable cv_;
        bool allocate_pending_{false};
        bool epoch_end_pending_{false};

        std::thread timer_thread_;
        std::thread worker_thread_;

        void run()
        {
            auto epoch_startup_tp = now_tp();
            const Timestamp allocation_time = (epoch_duration_ * 3) / 4;

            next_epoch_ = tp_to_timestamp(epoch_startup_tp + EpochDurationUnit(epoch_duration_));

            while (running_)
            {
                // interruptable sleep (allocation time or notify)
                cout << "[timer] before first wait" << endl;
                {
                    std::unique_lock<std::mutex> lock(work_mu_);
                    cv_.wait_until(lock,
                                   epoch_startup_tp + EpochDurationUnit(allocation_time),
                                   [this]
                                   { return !running_; });
                }
                cout << "[timer] after first wait" << endl;
                if (!running_)
                    break;

                cout << "[timer] on_allocate_ called" << endl;
                {
                    std::lock_guard<std::mutex> lock(work_mu_);
                    allocate_pending_ = true;
                }
                cv_.notify_one();
                cout << "[timer] on_allocate_ returned" << endl;

                cout << "[timer] before second wait" << endl;
                {
                    std::unique_lock<std::mutex> lock(work_mu_);
                    cv_.wait_until(lock,
                                   epoch_startup_tp + EpochDurationUnit(epoch_duration_),
                                   [this]
                                   { return !running_; });
                }
                cout << "[timer] after second wait" << endl;
                if (!running_)
                    break;

                {
                    std::lock_guard<std::mutex> lock(work_mu_);
                    epoch_end_pending_ = true;
                }
                cv_.notify_one();

                epoch_startup_tp += EpochDurationUnit(epoch_duration_); // no drift after thread detach
                next_epoch_ += epoch_duration_;
            }
        }

        void work_loop()
        {
            while (running_)
            {
                std::unique_lock<std::mutex> lock(work_mu_);
                cv_.wait(lock, [this]
                         { return allocate_pending_ || epoch_end_pending_ || !running_; });
                if (!running_)
                    break;

                if (allocate_pending_)
                {
                    allocate_pending_ = false;
                    lock.unlock();
                    cout << "[work loop] on allocate called" << endl;
                    on_allocate_();
                    cout << "[work loop] on allocate returned" << endl;
                }
                else if (epoch_end_pending_)
                {
                    epoch_end_pending_ = false;
                    lock.unlock();
                    cout << "[work loop] on epoch end called" << endl;
                    on_epoch_end_();
                    cout << "[work loop] on epoch end returned" << endl;
                }
            }
        }
    };

} // namespace ziplog::impl