//
// Created by drobison on 6/1/21.
//
// Create a control taskflow that pulls from a shared priority queue while a shared state variable is true.
//
#include <taskflow/taskflow.hpp>  // Taskflow is header-only

void produce(std::priority_queue<int> &pq, const int &running) {
    std::cout << "Producer starting\n";
    while (running) {
        auto val = std::rand() % 100;
        pq.push(val);

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    std::cout << "Producer Exiting\n";
}

int main() {
    int timeout = 30;
    int running = 1;
    std::srand(std::time(nullptr));
    tf::Executor executor;
    tf::Taskflow taskflow;

    tf::Task init = taskflow.emplace([]() { std::cout << "Starting Control Flow\n"; }).name("init");
    tf::Task stop = taskflow.emplace([]() { std::cout << "Stopping Control Flow\n"; }).name("stop");

    auto pq = std::priority_queue<int>();
    // Launch producer thread
    std::thread producer_thread(produce, std::ref(pq), std::ref(running));

    // Launch thread that runs for a fixed length of time and then sets the exit flag.
    std::thread timeout_thread([&running, timeout](){
        std::cout << "Starting timeout thread\n";
        std::this_thread::sleep_for(std::chrono::seconds(timeout));
        running = 0;
        std::cout << "Stopping timeout thread\n";
    });

// creates a condition task that returns a random binary
    tf::Task cond = taskflow.emplace([&pq, &running](){
        while (!pq.empty()) {
            std::cout << "Pulled from queue: " << pq.top() << std::endl;
            pq.pop();
        }

        return (running == 0);
    }).name("cond");

// creates a feedback loop {0: cond, 1: stop}
    init.precede(cond);
    cond.precede(cond, stop);  // moves on to 'cond' on returning 0, or 'stop' on 1

    executor.run(taskflow).wait();
    timeout_thread.join();
    producer_thread.join();

    return 0;
}