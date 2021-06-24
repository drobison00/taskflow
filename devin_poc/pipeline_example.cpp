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
    p.add_node("file", "source_input", "string", {}, [](){})
        .add_node("map", "map_to_json_1", "json", {"source_input"}, []() {})
        .add_node("map", "map_to_json_2", "json", {"source_input"}, []() {})
        .add_node("map", "map_json_to_json_1", "json", {"map_to_json_1", "map_to_json_2"}, []() {})
        .add_node("map", "map_json_to_json_2", "json", {"map_to_json_2", "map_to_json_1"}, []() {})
        .add_node("map", "map_json_to_json_3", "json", {"map_to_json_2", "map_json_to_json_1", "map_json_to_json_2"},
                  []() {})
        .add_node("map", "map_json_to_json_4", "json", {"map_to_json_1", "map_json_to_json_2", "map_to_json_2"},
                  []() {})
        .add_node("sink", "sink1", "none", {"map_json_to_json_3"}, []() {})
        .add_node("sink", "sink2", "none", {"map_json_to_json_4"}, []() {})
        .build()
        .name("Example taskflow")
        .visualize("graph.dot")
        .start(timeout);
}