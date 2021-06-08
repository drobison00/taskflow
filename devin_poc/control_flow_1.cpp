//
// Created by drobison on 6/1/21.
//
#include <taskflow/taskflow.hpp>  // Taskflow is header-only

int main() {
    std::srand(std::time(nullptr));
    tf::Executor executor;
    tf::Taskflow taskflow;

    tf::Task init = taskflow.emplace([]() { std::cout << "Starting Control Flow\n"; }).name("init");
    tf::Task stop = taskflow.emplace([]() { std::cout << "Stopping Control Flow\n"; }).name("stop");

// creates a condition task that returns a random binary
    tf::Task cond = taskflow.emplace([]() {
        auto binval = std::rand() % 2;
        std::cout << "Binary draw: " << binval << std::endl;

        return binval;
    }).name("cond");

// creates a feedback loop {0: cond, 1: stop}
    init.precede(cond);
    cond.precede(cond, stop);  // moves on to 'cond' on returning 0, or 'stop' on 1


    executor.run(taskflow).wait();

    return 0;
}