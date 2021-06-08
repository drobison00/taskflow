//
// Created by drobison on 6/1/21.
//
// Create a control taskflow that launches and processes all elements of a priority queue before exiting.
//
#include <taskflow/taskflow.hpp>  // Taskflow is header-only

int main() {
    std::srand(std::time(nullptr));
    tf::Executor executor;
    tf::Taskflow taskflow;

    tf::Task init = taskflow.emplace([]() { std::cout << "Starting Control Flow\n"; }).name("init");
    tf::Task stop = taskflow.emplace([]() { std::cout << "Stopping Control Flow\n"; }).name("stop");

    auto pq = std::priority_queue<int>();
    for (int i; i < 10; i++) {
        pq.push(std::rand() % 100);
    }

// creates a condition task that returns a random binary
    tf::Task cond = taskflow.emplace([&pq]() {
        int val = pq.top();
        pq.pop();
        std::cout << "Pulled from queue: " << val << std::endl;

        return (pq.empty());
    }).name("cond");

// creates a feedback loop {0: cond, 1: stop}
    init.precede(cond);
    cond.precede(cond, stop);  // moves on to 'cond' on returning 0, or 'stop' on 1


    executor.run(taskflow).wait();

    return 0;
}