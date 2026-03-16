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
        ProxyEpochTimer(EpochDurationUnit epoch_duration,
                        std::function<void()> on_epoch_end,
                        std::function<Timestamp()> next_batch_timestamp,
                        std::function<void()> on_send_batch)
            : epoch_duration_(epoch_duration.count()), on_epoch_end_(std::move(on_epoch_end)), next_batch_timestamp_(std::move(next_batch_timestamp)), on_send_batch_(std::move(on_send_batch)) {}

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
        Timestamp epoch_duration_;
        std::function<void()> on_epoch_end_;
        std::function<Timestamp()> next_batch_timestamp_;
        std::function<void()> on_send_batch_;

        std::atomic<bool> running_{false};
        std::atomic<bool> paused_{false};
        std::thread thread_;

        void run()
        {
            auto epoch_end_tp = now_tp() + EpochDurationUnit(epoch_duration_);

            while (running_)
            {
                Timestamp next_batch_send = next_batch_timestamp_();

                // either sleep until batch is ready to be sent or the end of your epoch
                if (next_batch_send != 0)
                {
                    auto batch_send_tp = timestamp_to_tp(next_batch_send);
                    std::this_thread::sleep_until(std::min(batch_send_tp, epoch_end_tp));
                }
                else
                {
                    std::this_thread::sleep_until(epoch_end_tp);
                }

                if (paused_)
                    continue;

                if (now_tp() >= epoch_end_tp)
                {
                    on_epoch_end_();
                    epoch_end_tp += EpochDurationUnit(epoch_duration_); // avoid drift
                }
                else
                {
                    std::thread(on_send_batch_).detach();
                }
                        }
        }
    };

} // namespace ziplog::impl