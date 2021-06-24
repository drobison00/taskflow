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
    int workers = 4;

    if (argc > 3) {
    }

    tf::Executor executor(workers);

    Pipeline p(executor);
    p.source("file", "devin_poc/without_data_len.json", "source_input")
        .map("map_to_json_1", {"source_input"}, map_parse_to_json)
        .map("map_to_json_2", {"source_input"}, map_parse_to_json)
        .map("map_json_to_json_1", {"map_to_json_1", "map_to_json_2"}, map_merge_json)
        .map("map_json_to_json_2", {"map_to_json_2", "map_to_json_1"}, map_merge_json)
        .map("map_json_to_json_3",
                  {"map_to_json_2", "map_json_to_json_1", "map_json_to_json_2"},
                  map_merge_json)
        .map("map_json_to_json_4",
                  {"map_to_json_1", "map_json_to_json_2", "map_to_json_2"},
                  map_merge_json)
        .map("map_json_to_json_5",
                  {"map_to_json_1", "map_json_to_json_2", "map_to_json_2"},
                  map_merge_json)
        .map("map_json_to_json_6",
                  {"map_to_json_1", "map_json_to_json_2", "map_to_json_2"},
                  map_merge_json)
        .map("map_json_to_json_7",
                  {"map_to_json_1", "map_json_to_json_2", "map_to_json_2"},
                  map_merge_json)
        .sink("sink1", {"map_json_to_json_3", "map_json_to_json_5", "map_json_to_json_6"},
                  sink_passthrough)
        .sink("sink2", {"map_json_to_json_7", "map_json_to_json_4"}, sink_passthrough)
        .build()
        .name("Example taskflow")
        .visualize("graph.dot")
        .start(timeout);
}