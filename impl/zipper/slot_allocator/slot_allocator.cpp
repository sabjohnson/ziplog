#include "slot_allocator.h"
#include <algorithm>
#include <iostream>

using namespace ziplog::api;

namespace ziplog::impl
{

    SlotAllocator::SlotAllocator(ProxyRegistry &registry, SequenceNumber starting_seq)
        : registry_(registry), global_seq_num_(starting_seq) {}

    void SlotAllocator::update_estimate(NodeId proxy_id, size_t num_requests)
    {
        std::lock_guard<std::mutex> lock(registry_.mutex());
        if (!registry_.exists(proxy_id))
            return;
        registry_.get(proxy_id).estimate = num_requests;
    }

    std::unordered_map<NodeId, std::vector<SequenceNumber>>
    SlotAllocator::compute_allocations(Timestamp next_epoch, Timestamp epoch_duration_ms)
    {
        std::lock_guard<std::mutex> lock(registry_.mutex());

        // build sorted timestamp list across all active proxies
        std::vector<std::pair<double, NodeId>> timestamps;

        for (auto &[proxy_id, state] : registry_.all())
        {
            if (state.status != ProxyStatus::ACTIVE)
                continue;
            if (state.estimate == 0)
                continue;

            double interval = static_cast<double>(epoch_duration_ms) / state.estimate;
            double time_point = interval / 2.0;

            for (size_t i = 0; i < state.estimate; i++)
            {
                timestamps.push_back({time_point, proxy_id});
                time_point += interval;
            }
        }

        std::sort(timestamps.begin(), timestamps.end());

        // assign sequence numbers
        std::unordered_map<NodeId, std::vector<SequenceNumber>> result;
        for (const auto &[time_point, proxy_id] : timestamps)
        {
            SequenceNumber seq = global_seq_num_++;
            result[proxy_id].push_back(next_epoch + static_cast<Timestamp>(time_point));
            result[proxy_id].push_back(seq);
            registry_.get(proxy_id).allocated_sequences.push_back(seq);
        }

        return result;
    }

} // namespace ziplog::impl