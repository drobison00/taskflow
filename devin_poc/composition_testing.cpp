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

#include <linearpipeline.hpp>
#include <boost/variant.hpp>

using namespace std::chrono;
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

    boost::variant< std::string, json, int, double > dtype;
    tf::Executor executor(workers);
    LinearPipeline lp(executor, true);

    auto x = std::map<std::string,
        std::tuple<
            boost::variant<std::string, json, int, double>,
            boost::variant<std::string, json, int, double>
        >>();

    auto thing = make_kv_pair("int_vec", std::vector<int>());
    std::cout << type_name<decltype(thing)>() << std::endl;
    std::cout << thing.key() << std::endl;
    std::cout << type_name< decltype(thing.value())>() << std::endl;

    Pipeline p(executor);
    p.add_node("source_input", "string", {}, [](){} )
        .add_node("map_to_json_1", "json", {"source_input"}, [](std::string *s){
            return new json(json::parse(*s));
        })
        .add_node("map_to_json_2", "json", {"source_input"}, [](){})
        .add_node("map_json_to_json_1", "json", {"map_to_json_1", "map_to_json_2"}, [](){})
        .add_node("map_json_to_json_2", "json", {"map_to_json_2", "map_to_json_1"}, [](){})
        .add_node("map_json_to_json_3", "json", {"map_to_json_2", "map_json_to_json_1", "map_json_to_json_2"},
                  [](){})
        .add_node("map_json_to_json_4", "json", {"map_to_json_1", "map_json_to_json_2", "map_to_json_2"},
                  [](){})
        .add_node("sink1", "none", {"map_json_to_json_3"}, [](){})
        .add_node("sink2", "none", {"map_json_to_json_4"}, [](){})
        .build()
        .name("Example taskflow")
        .visualize("graph.dot")
        .start(300);

    lp.source(std::string("file"), std::string("devin_poc/without_data_len.json"))
     .map(map_string_to_json)
     .map(map_random_work_on_json_object)                   // Perform various operations on each JSON object
     .map(map_random_trig_work_json)                        // Do intensive trig work and forward JSON packets
     .map(map_random_trig_work_json)                        // Do intensive trig work and forward JSON packets
     .explode(exploder_duplicate_json)                      // Duplicate every JSON object 10x
     .filter(filter_random_drop<json>)                      // Randomly drop 50% of packets
     .map(map_random_trig_work_json)                        // Do intensive trig work and forward JSON packets
     .explode(exploder_duplicate<json>)                     // Duplicated every JSON object 10x
     .batch<json>(10, 10)                                   // Batch 10 JSON objects at a time and forward
     .sink(sink_discard<Batch<json>>)                       // Sink all packets
     .add_conditional_stage(map_conditional_jump_to_start)  // Taskflow loopback
     .visualize("main_graph.dot")
     .start(timeout);

    std::cout << "Stopped" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));

    return 0;
}