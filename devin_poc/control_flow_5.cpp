//
// Created by drobison on 6/1/21.
//
// Create a control taskflow that pulls from a shared priority queue while a shared state variable is true. Each pulled
//  value is then passed to an asynchronous taskflow handler for execution.
//
#include <taskflow/taskflow.hpp>  // Taskflow is header-only


std::mutex m;

void produce(std::queue<int> &pq, const int rate_per_sec, const int &running) {
    std::cout << "Producer starting\n";
    auto rps = rate_per_sec;

    while (running) {
        using namespace std::chrono;


        high_resolution_clock::time_point start = high_resolution_clock::now();
        for (int i = 0; i < rps && running; i++) {
            {
                std::lock_guard <std::mutex> lock(m);
                pq.push(int(std::rand() % 100));
            }
        }
        high_resolution_clock::time_point end = high_resolution_clock::now();
        duration<double> time_span = duration_cast<duration<double>>(end - start);
        duration<double> time_span_max(1.0);

        if (time_span > time_span_max) {
            std::cout << "Adjusting timespan" << std::endl;
            auto rps_old = rps;
            rps = rps * (1 / time_span.count()) * 0.95;
            std::cout << "Unable to satisfy specified rate (" << rps_old << " /sec), scaling to ("
            << rps << " /sec)" << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    std::cout << "Producer Exiting, draining queue.\n";
    while (pq.size() > 0) { pq.pop(); }
};


int main() {
    int rate_per_sec = 1000001;
    int timeout = 30;
    int running = 1;
    std::srand(std::time(nullptr));
    tf::Executor executor;
    tf::Taskflow taskflow;

    tf::Task init = taskflow.emplace([]() { std::cout << "Starting Control Flow\n"; }).name("init");
    tf::Task stop = taskflow.emplace([]() { std::cout << "Stopping Control Flow\n"; }).name("stop");

    std::queue<int> pq;
    // Launch producer thread
    std::thread producer_thread(produce, std::ref(pq), rate_per_sec,  std::ref(running));

    // Launch thread that runs for a fixed length of time and then sets the exit flag.
    std::thread timeout_thread([&running, timeout](){
        std::cout << "Starting timeout thread\n";
        std::this_thread::sleep_for(std::chrono::seconds(timeout));
        running = 0;
        std::cout << "\nStopping timeout thread\n";
    });

// creates a condition task that returns a random binary
    tf::Task cond = taskflow.emplace([&pq, &running](){
        static int processed = 0;

        while (running) {
            int val;
            {
                std::lock_guard<std::mutex> lock(m);
                if (pq.empty()) { break; }
                val = pq.front();
                pq.pop();
            }

            processed++;
            if (processed % 1000000 == 0) {
                std::cout << "." << std::flush;
            }
            // TODO: Implement some type of back-pressure system.
        }

        return (running == 0);
    }).name("cond");

// creates a feedback loop {0: cond, 1: stop}
    init.precede(cond);
    cond.precede(cond, stop);  // moves on to 'cond' on returning 0, or 'stop' on 1

    executor.run(taskflow).wait();
    std::cout << "\n" << std::flush;
    timeout_thread.join();
    producer_thread.join();

    return 0;
}