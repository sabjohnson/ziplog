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

    class ProxyEpochTimer
    {
    public:
        ProxyEpochTimer(Timestamp epoch_duration_ms,
                        std::function<void()> on_epoch_end,
                        std::function<bool()> should_send_batch,
                        std::function<void()> on_send_batch)
            : epoch_duration_ms_(epoch_duration_ms), on_epoch_end_(std::move(on_epoch_end)), should_send_batch_(std::move(should_send_batch)), on_send_batch_(std::move(on_send_batch)) {}

        ~ProxyEpochTimer() { stop(); }

        void start()
        {
            running_ = true;
            thread_ = std::thread(&ProxyEpochTimer::run, this);
            cout << "proxy epoch timer started" << endl;
        }

        void stop()
        {
            running_ = false;
            if (thread_.joinable())
                thread_.join();
        }

        void pause() { paused_ = true; }
        void resume() { paused_ = false; }

    private:
        void run()
        {
            Timestamp epoch_startup = now_ms();
            const Timestamp sleep_ms = std::max(Timestamp(1), epoch_duration_ms_ / 200);

            while (running_)
            {
                if (!paused_)
                {
                    Timestamp now = now_ms();
                    Timestamp elapsed = now - epoch_startup;

                    if (elapsed >= epoch_duration_ms_)
                    {
                        epoch_startup = now_ms();
                        on_epoch_end_();
                    }
                    else if (should_send_batch_())
                    {
                        std::thread(on_send_batch_).detach();
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
            }
        }

        Timestamp epoch_duration_ms_;
        std::function<void()> on_epoch_end_;
        std::function<bool()> should_send_batch_;
        std::function<void()> on_send_batch_;

        std::atomic<bool> running_{false};
        std::atomic<bool> paused_{false};
        std::thread thread_;
    };

} // namespace ziplog::impl