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

    unsigned int timeout = 10;
    if (argc > 2) {
        timeout = std::stoi(argv[2]);
    }
    int workers = 16;

    tf::Executor executor(workers);
    LinearPipeline lp(executor, true);

    lp.set_source<std::fstream, std::string>(std::string("devin_poc/without_data_len.json"), rate_per_sec)
     .filter(filter_random_drop)                // Randomly drop 50% of packets
     .map(map_string_to_json)                   // Parse strings into JSON objects
     .map(map_random_work_on_json_object)       // Perform various operations on each JSON object
     .map(map_random_trig_work<json>)           // Do intensive trig work and forward JSON packets
     .explode(exploder_duplicate<json>)         // Duplicate every JSON object 10x
     .filter(filter_random_drop)                // Randomly drop 50% of packets
     .map(map_random_trig_work<json>)           // Do intensive trig work and forward JSON packets
     .explode(exploder_duplicate<json, json>)   // Duplicated every JSON object 10x
     .batch(10, 10)                             // Batch 10 JSON objects at a time and forward
     .set_sink(std::string(""), sink_discard)   // Sink all packets
     .add_conditional_stage(map_conditional_jump_to_start) // Taskflow loopback
     .visualize("main_graph.dot")
     .start(timeout);

    std::cout << "Stopped" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));

    return 0;
}