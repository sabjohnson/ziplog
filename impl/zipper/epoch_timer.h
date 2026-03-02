#pragma once
#include "../api/types.h"
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
        ZipperEpochTimer(Timestamp epoch_duration_ms,
                         std::function<void()> on_allocate,
                         std::function<void()> on_epoch_end)
            : epoch_duration_ms_(epoch_duration_ms), on_allocate_(std::move(on_allocate)), on_epoch_end_(std::move(on_epoch_end)) {}

        ~ZipperEpochTimer() { stop(); }

        void start()
        {
            running_ = true;
            thread_ = std::thread(&ZipperEpochTimer::run, this);
        }

        void stop()
        {
            running_ = false;
            if (thread_.joinable())
                thread_.join();
        }

        Timestamp next_epoch() const { return next_epoch_; }

    private:
        void run()
        {
            Timestamp epoch_startup = now_ms();
            next_epoch_ = epoch_startup + epoch_duration_ms_;

            const Timestamp allocation_time = (epoch_duration_ms_ * 3) / 4;
            const Timestamp allocation_buffer = std::max(Timestamp(1), epoch_duration_ms_ / 100);
            const Timestamp sleep_duration = std::max(Timestamp(1), epoch_duration_ms_ / 200);

            while (running_)
            {
                Timestamp elapsed = now_ms() - epoch_startup;

                if (elapsed >= allocation_time && elapsed < allocation_time + allocation_buffer)
                {
                    on_allocate_();
                    std::this_thread::sleep_for(10ms);
                }

                if (elapsed >= epoch_duration_ms_)
                {
                    std::thread(on_epoch_end_).detach();
                    epoch_startup = now_ms();
                    next_epoch_ = epoch_startup + epoch_duration_ms_;
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
            }
        }

        Timestamp epoch_duration_ms_;
        std::function<void()> on_allocate_;
        std::function<void()> on_epoch_end_;

        std::atomic<Timestamp> next_epoch_{0};
        std::atomic<bool> running_{false};
        std::thread thread_;
    };

} // namespace ziplog::impl