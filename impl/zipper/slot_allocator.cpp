#include "zipper/slot_allocator.h"
#include <algorithm>
#include <iostream>

using namespace ziplog::api;

namespace ziplog::impl
{

    SlotAllocator::SlotAllocator(ProxyRegistry &registry, SequenceNumber starting_seq)
        : registry_(registry), global_seq_num_(starting_seq) {}

    void SlotAllocator::update_estimate(NodeId proxy_id, size_t num_requests)
    {
        cout << "[update_estimate] waiting for lock..." << endl;
        std::lock_guard<std::mutex> lock(registry_.mutex());
        cout << "[update_estimate] got lock" << endl;
        if (!registry_.exists_unlocked(proxy_id))
        {
            cout << "zipper cant update estimate because proxy not registered" << endl;
            return;
        }
        registry_.get_unlocked(proxy_id).estimate = num_requests;
        cout << "zipper update estimate exited" << endl;
    }

    std::unordered_map<NodeId, std::vector<SequenceNumber>>
    SlotAllocator::compute_allocations(Timestamp next_epoch, Timestamp epoch_duration_ms)
    {
        cout << "[slot_allocator] waiting for lock..." << endl;
        std::lock_guard<std::mutex> lock(registry_.mutex());
        cout << "[slot_allocator] got lock" << endl;

        // build sorted timestamp list across all active proxies
        std::vector<std::pair<double, NodeId>> timestamps;

        for (auto &[proxy_id, state] : registry_.all())
        {
            cout << "proxy " << proxy_id << " status=" << (int)state.status << " estimate=" << state.estimate << endl;

            if (state.status != ProxyStatus::ACTIVE)
            {
                cout << "proxy not active skipping" << endl;
                continue;
            }
            if (state.estimate == 0)
            {
                cout << "proxy doesnt want slots" << endl;
                continue;
            }

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

        cout << "[zipper] compute_allocations() exited" << endl;
        return result;
    }

} // namespace ziplog::impl