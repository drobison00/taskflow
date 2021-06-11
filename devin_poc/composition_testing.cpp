//
// Created by drobison on 6/1/21.
//
// Create a control taskflow that pulls from a shared priority queue while a shared state variable is true. Each pulled
//  value is then passed to an asynchronous taskflow handler for execution.
//
#include <assert.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <string>

#include <building_blocks.hpp>

using namespace std::chrono;
using json = nlohmann::json;

void test_func_1() {
    std::cout << "test" << std::endl;
}

int main(int argc, char **argv) {
    unsigned int rate_per_sec = 1;
    if (argc > 1) {
        rate_per_sec = std::stoi(argv[1]);
    }
    unsigned int timeout = 300;
    int workers = 16;

    tf::Executor executor(workers);

    LinearPipeline lp(executor, true);

    lp.set_source<std::fstream, std::string>("devin_poc/without_data_len.json", rate_per_sec);
    lp.add_stage(std::function<json(std::string)>(work_routine_string_to_json));
    lp.add_stage(std::function<json(json)>(work_routine_random_work_on_json_object));
    lp.add_stage(std::function<json(json)>(work_routine_random_work_on_json_object));
    lp.add_stage(std::function<json(json)>(work_routine_random_work_on_json_object));
    lp.add_stage(std::function<json(json)>(work_routine_random_work_on_json_object));
    lp.set_sink<json, SinkThrowAway<json>>("");
    lp.add_conditional_stage(work_routine_conditional_jump_to_start);
    lp.start();

    return 0;
}