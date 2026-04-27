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
                        std::function<bool(Timestamp &, SequenceNumber &)> next_batch_info,
                        std::function<void(SequenceNumber)> on_send_batch,
                        std::function<void(Timestamp, SequenceNumber)> push_front_slot)
            : epoch_duration_(epoch_duration.count()), on_epoch_end_(std::move(on_epoch_end)), next_batch_info_(std::move(next_batch_info)), on_send_batch_(std::move(on_send_batch)), push_front_slot_(std::move(push_front_slot)) {}

        ~ProxyEpochTimer() { stop(); }

        void start()
        {
            running_ = true;
            thread_ = std::thread(&ProxyEpochTimer::run, this);
            ZLOG("proxy epoch timer started");
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
        std::function<bool(Timestamp &, SequenceNumber &)> next_batch_info_;
        std::function<void(SequenceNumber)> on_send_batch_;
        std::function<void(Timestamp, SequenceNumber)> push_front_slot_;

        std::atomic<bool> running_{false};
        std::atomic<bool> paused_{false};
        std::thread thread_;

        void run()
        {
            auto epoch_end_tp = now_tp() + EpochDurationUnit(epoch_duration_);

            while (running_)
            {
                if (paused_)
                    break;

                on_epoch_end_(); // generate slots for this epoch

                Timestamp batch_ts = 0;
                SequenceNumber batch_seq = 0;

                // drain all slots for this epoch
                while (next_batch_info_(batch_ts, batch_seq))
                {
                    auto batch_send_tp = timestamp_to_tp(batch_ts);

                    if (batch_send_tp > epoch_end_tp)
                    {
                        // slot belongs to next epoch — but we already popped it
                        // re-insert it back into the scheduler
                        push_front_slot_(batch_ts, batch_seq);
                        break;
                    }

                    std::this_thread::sleep_until(batch_send_tp);
                    std::thread([this, batch_seq]()
                                { on_send_batch_(batch_seq); })
                        .detach();
                }

                std::this_thread::sleep_until(epoch_end_tp);
                epoch_end_tp += EpochDurationUnit(epoch_duration_);
            }
        }

        /*
        void run()
        {
            auto epoch_end_tp = now_tp() + EpochDurationUnit(epoch_duration_);

            bool outgoing_batch = false;
            Timestamp batch_ts = 0;
            SequenceNumber batch_seq = 0;

            while (running_)
            {
                if (paused_)
                {
                    break;
                    // TODO: change to wait on condition variable
                }

                // either sleep until batch is ready to be sent or the end of your epoch
                outgoing_batch = next_batch_info_(batch_ts, batch_seq);
                if (outgoing_batch)
                {
                    ZLOG("[proxy epoch timer] send out batch incoming: " << batch_ts);
                    auto batch_send_tp = timestamp_to_tp(batch_ts);
                    std::this_thread::sleep_until(std::min(batch_send_tp, epoch_end_tp));
                }
                else
                {
                    std::this_thread::sleep_until(epoch_end_tp);
                }

                if (outgoing_batch)
                {
                    std::thread([this, batch_seq]()
                                { on_send_batch_(batch_seq); })
                        .detach();
                }
                on_epoch_end_();
                epoch_end_tp += EpochDurationUnit(epoch_duration_); // avoid drift
            }
        }
            */
    };

} // namespace ziplog::impl