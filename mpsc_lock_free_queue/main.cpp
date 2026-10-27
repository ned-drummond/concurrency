#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>

static constexpr size_t NUM_ITEMS = 100'000'000;
static constexpr size_t NUM_RUNS = 3;
static constexpr size_t QUEUE_SIZE = 4'096;

template <typename T, size_t Capacity>
class MpscQueue {
public:
    static_assert(std::has_single_bit(Capacity), "Capacity must be a power of 2");

    struct alignas(std::hardware_destructive_interference_size) Slot {
        std::atomic<size_t> seq;
        T data;
    };

    MpscQueue() {
        // Initialize each slot to its empty state
        for (size_t i = 0; i < Capacity; ++i) {
            _queue[i].seq.store(i, std::memory_order_relaxed);
        }
    }

    // Called only from the producer threads.
    // Returns false immediately if the queue is full.
    bool try_push(const T& value);

    // Called only from the consumer thread.
    // Returns false immediately if the queue is empty.
    bool try_pop(T& out);

private:
    // Written by every producer
    alignas(std::hardware_destructive_interference_size) std::atomic<size_t> _next_available_slot{0};

    alignas(std::hardware_destructive_interference_size) Slot _queue[Capacity];

    // Used only by the consumer
    alignas(std::hardware_destructive_interference_size) size_t _consumer_index = 0;
};

template<typename T, size_t Capacity>
bool MpscQueue<T, Capacity>::try_push(const T& value) {
    auto pos = _next_available_slot.load(std::memory_order_relaxed);

    for (;;) {
        Slot& slot = _queue[pos & (Capacity - 1)];
        const auto seq = slot.seq.load(std::memory_order_acquire);
        const auto diff = static_cast<std::ptrdiff_t>(seq - pos);

        if (diff == 0) {
            // Free for this position: try to claim it
            if (_next_available_slot.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                slot.data = value;
                slot.seq.store(pos + 1, std::memory_order_release);
                return true;
            }
            // Lost the race (or a spurious failure): pos now holds the current counter
        } else if (diff < 0) {
            return false; // Still holds last lap's item: full
        } else {
            pos = _next_available_slot.load(std::memory_order_relaxed);  // Stale: catch up
        }
    }
}

template<typename T, size_t Capacity>
bool MpscQueue<T, Capacity>::try_pop(T& out) {
    if (_queue[_consumer_index & (Capacity - 1)].seq.load(std::memory_order_acquire) != _consumer_index + 1) return false;

    out = _queue[_consumer_index & (Capacity - 1)].data;
    _queue[_consumer_index & (Capacity - 1)].seq.store(_consumer_index + Capacity, std::memory_order_release);
    ++_consumer_index;
    return true;
}

int main() {
    double total_ns = 0.0;
    for (size_t i = 0; i < NUM_RUNS; ++i) {
        MpscQueue<int, QUEUE_SIZE> queue;
        const auto start = std::chrono::steady_clock::now();

        std::thread producer_even([&] {
            for (size_t i = 0; i < NUM_ITEMS; i += 2) {
                while (!queue.try_push(static_cast<int>(i))) {}
            }
        });

        std::thread producer_odd([&] {
            for (size_t i = 1; i < NUM_ITEMS; i += 2) {
                while (!queue.try_push(static_cast<int>(i))) {}
            }
        });

        std::thread consumer([&] {
            int item {};
            int next_even = 0;
            int next_odd = 1;

            for (size_t i = 0; i < NUM_ITEMS; ++i) {
                while (!queue.try_pop(item)) {}
                if (item % 2 == 0) {
                    if (item != next_even) throw std::runtime_error("Bad ordering, i: " + std::to_string(i) + " item: " + std::to_string(item));
                    next_even = item + 2;
                } else {
                    if (item != next_odd) throw std::runtime_error("Bad ordering, i: " + std::to_string(i) + " item: " + std::to_string(item));
                    next_odd = item + 2;
                }
            }
        });

        producer_even.join();
        producer_odd.join();
        consumer.join();

        const auto stop = std::chrono::steady_clock::now();
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
        total_ns += ns;
        std::cout << static_cast<double>(ns) / NUM_ITEMS << " ns per item" << std::endl;
    }

    std::cout << "Average ns per item: " << total_ns / (NUM_ITEMS * NUM_RUNS) << std::endl;

    return 0;
}