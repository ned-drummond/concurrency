#include <atomic>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

static constexpr size_t NUM_ITEMS = 100'000'000;

template <typename T, size_t Capacity>
class SpscQueue {
public:
    // Called only from the producer thread.
    // Returns false immediately if the queue is full.
    bool try_push(const T& value);

    // Called only from the consumer thread.
    // Returns false immediately if the queue is empty.
    bool try_pop(T& out);

private:
    alignas(64) T _queue[Capacity];
    alignas(64) std::atomic<size_t> _producer_index = { 0 };
    alignas(64) std::atomic<size_t> _consumer_index = { 0 };
};

template<typename T, size_t Capacity>
bool SpscQueue<T, Capacity>::try_push(const T& value) {
    const auto producer_index = _producer_index.load(std::memory_order_relaxed);
    const auto consumer_index = _consumer_index.load(std::memory_order_acquire);

    size_t size = 0;
    if (producer_index > consumer_index) size = producer_index - consumer_index;
    else if (consumer_index > producer_index) size = Capacity - (consumer_index - producer_index);

    if (size == Capacity - 1) return false;

    _queue[producer_index] = value;
    _producer_index.store((producer_index + 1) % Capacity, std::memory_order_release);
    return true;
}

template<typename T, size_t Capacity>
bool SpscQueue<T, Capacity>::try_pop(T& out) {
    const auto producer_index = _producer_index.load(std::memory_order_acquire);
    const auto consumer_index = _consumer_index.load(std::memory_order_relaxed);

    if (producer_index == consumer_index) return false;

    out = _queue[consumer_index];
    _consumer_index.store((consumer_index + 1) % Capacity, std::memory_order_release);
    return true;
}

int main() {
    SpscQueue<int, 32> queue;
    auto start = std::chrono::steady_clock::now();

    std::thread producer([&] {
        for (size_t i = 0; i < NUM_ITEMS; ++i) {
            while (!queue.try_push(static_cast<int>(i))) {}
        }
    });

    std::thread consumer([&] {
        size_t items_received = 0;
        int prev_item {};
        int item {};

        while (items_received != NUM_ITEMS) {
            while (!queue.try_pop(item)) {}
            if (item != (prev_item + 1) && items_received != 0) throw std::runtime_error("Bad ordering. Item: " + std::to_string(item) + " prev item: " + std::to_string(prev_item));

            prev_item = item;
            ++items_received;
        }
    });

    producer.join();
    consumer.join();

    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - start).count();
    std::cout << double(ns) / NUM_ITEMS << "ns per item" << std::endl;

    return 0;
}