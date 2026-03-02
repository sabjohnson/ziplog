#include "epoch_timer.h"
#include "../api/types.h"

using namespace ziplog::api;

namespace ziplog::impl
{

    EpochTimer::EpochTimer(Timestamp duration,
                           std::function<void()> on_allocate,
                           std::function<void()> on_epoch_end)
        : epoch_duration_ms_(duration), on_allocate_(std::move(on_allocate)), on_epoch_end_(std::move(on_epoch_end)) {}

    EpochTimer::~EpochTimer() { stop(); }

    void EpochTimer::start()
    {
        running_ = true;
        thread_ = std::thread(&EpochTimer::run, this);
    }

    void EpochTimer::stop()
    {
        running_ = false;
        if (thread_.joinable())
            thread_.join();
    }

    void EpochTimer::run()
    {
        epoch_startup_ = now_ms();
        next_epoch_ = epoch_startup_ + epoch_duration_ms_;

        const Timestamp allocation_time = (epoch_duration_ms_ * 3) / 4;
        const Timestamp allocation_buffer = std::max(Timestamp(1), epoch_duration_ms_ / 100);
        const Timestamp sleep_duration = std::max(Timestamp(1), epoch_duration_ms_ / 200);

        while (running_)
        {
            Timestamp elapsed = now_ms() - epoch_startup_;

            if (elapsed >= allocation_time && elapsed < allocation_time + allocation_buffer)
            {
                on_allocate_();
                std::this_thread::sleep_for(10ms);
            }

            if (elapsed >= epoch_duration_ms_)
            {
                std::thread(on_epoch_end_).detach();
                epoch_startup_ = now_ms();
                next_epoch_ = epoch_startup_ + epoch_duration_ms_;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_duration));
        }
    }

} // namespace ziplog::impl