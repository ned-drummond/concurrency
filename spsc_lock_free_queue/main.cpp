#include <atomic>
#include <bit>
#include <chrono>
#include <iostream>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>

static constexpr size_t NUM_ITEMS = 100'000'000;
static constexpr size_t NUM_RUNS = 3;
static constexpr size_t QUEUE_SIZE = 16'384;

template <typename T, size_t Capacity>
class SpscQueue {
public:
    static_assert(std::has_single_bit(Capacity), "Capacity must be a power of 2");

    // Called only from the producer thread.
    // Returns false immediately if the queue is full.
    bool try_push(const T& value);

    // Called only from the consumer thread.
    // Returns false immediately if the queue is empty.
    bool try_pop(T& out);

    size_t size() const;

private:
    // Written only by the producer (both vars on same cache line)
    alignas(std::hardware_destructive_interference_size) std::atomic<size_t> _total_pushed = { 0 };
    size_t _last_total_popped = 0;

    // Written only by the consumer (both vars on same cache line)
    alignas(std::hardware_destructive_interference_size) std::atomic<size_t> _total_popped = { 0 };
    size_t _last_total_pushed = 0;

    alignas(std::hardware_destructive_interference_size) T _queue[Capacity];
};

template<typename T, size_t Capacity>
bool SpscQueue<T, Capacity>::try_push(const T& value) {
    const auto total_pushed = _total_pushed.load(std::memory_order_relaxed);

    if (total_pushed - _last_total_popped == Capacity) {
        _last_total_popped = _total_popped.load(std::memory_order_acquire);
        if (total_pushed - _last_total_popped == Capacity) return false;
    }

    _queue[total_pushed & (Capacity - 1)] = value;
    _total_pushed.store(total_pushed + 1, std::memory_order_release);
    return true;
}

template<typename T, size_t Capacity>
bool SpscQueue<T, Capacity>::try_pop(T& out) {
    const auto total_popped = _total_popped.load(std::memory_order_relaxed);

    if (_last_total_pushed == total_popped) {
        _last_total_pushed = _total_pushed.load(std::memory_order_acquire);
        if (_last_total_pushed == total_popped) return false;
    }

    out = _queue[total_popped & (Capacity - 1)];
    _total_popped.store(total_popped + 1, std::memory_order_release);
    return true;
}

template<typename T, size_t Capacity>
size_t SpscQueue<T, Capacity>::size() const {
    const auto popped = _total_popped.load(std::memory_order_acquire);
    const auto pushed = _total_pushed.load(std::memory_order_relaxed);
    return pushed - popped;
}

int main() {
    double total_ns = 0.0;
    for (size_t i = 0; i < NUM_RUNS; ++i) {
        SpscQueue<int, QUEUE_SIZE> queue;
        const auto start = std::chrono::steady_clock::now();

        std::thread producer([&] {
            for (size_t i = 0; i < NUM_ITEMS; ++i) {
                while (!queue.try_push(static_cast<int>(i))) {}
            }
        });

        std::thread consumer([&] {
            int item {};
            for (size_t i = 0; i < NUM_ITEMS; ++i) {
                while (!queue.try_pop(item)) {}
                if (item != static_cast<int>(i)) throw std::runtime_error("Bad ordering, i: " + std::to_string(i));
            }
        });

        producer.join();
        consumer.join();

        const auto stop = std::chrono::steady_clock::now();
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
        total_ns += ns;
        std::cout << static_cast<double>(ns) / NUM_ITEMS << " ns per item" << std::endl;
    }

    std::cout << "Average ns per item: " << total_ns / (NUM_ITEMS * NUM_RUNS) << std::endl;

    return 0;
}