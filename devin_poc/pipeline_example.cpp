//
// Created by drobison on 6/23/21.
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

#include <pipeline.hpp>
#include <boost/variant.hpp>

using namespace std::chrono;
using namespace taskflow_pipeline;
using json = nlohmann::json;

int main(int argc, char **argv) {
    unsigned int rate_per_sec = 1;
    if (argc > 1) {
        rate_per_sec = std::stoi(argv[1]);
    }

    unsigned int timeout = 10;
    if (argc > 2) {
        timeout = std::stoi(argv[2]);
    }
    int workers = 1;

    if (argc > 3) {
    }

    tf::Executor executor(workers);


    Pipeline p(executor);
    p.source("file", "devin_poc/without_data_len.json", "source_input")
        .map("parse_str_to_json", {"source_input"}, map_parse_to_json)
        //.flat_map("duplicate_json_10x", {"parse_str_to_json"}, flat_map_duplicate)
        .map("json_mutate", {"parse_str_to_json"}, map_random_work_on_json_object)
        .map("trig_work_and_forward_1", {"json_mutate"}, map_random_trig_work_and_forward)
         //.flat_map("duplicate_json_10x_2", {"trig_work_and_forward_1"}, flat_map_duplicate)
        .filter("random_drop_filter_1", {"trig_work_and_forward_1"}, filter_random_drop)
        //.map("trig_work_and_forward_2", {"random_drop_filter_1"}, map_random_trig_work_and_forward)
        .flat_map("duplicate_json_10x_3", {"random_drop_filter_1"}, flat_map_duplicate)
        .batch("batch_1", {"duplicate_json_10x_3"}, 10, 10)
        .sink("sink_1", {"batch_1"}, sink_passthrough)
        .build()
        .name("Example taskflow")
        .visualize("graph.dot")
        .start(timeout);

        std::this_thread::sleep_for(std::chrono::seconds(5));
}