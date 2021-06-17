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

    if (argc > 3) {
    }


    PipelineStageConstructor *psc = PipelineStageConstructor::get();

    psc->init_stage_constructors<std::string, json>(
            std::string("string"), std::string("json"));

    //auto success = psc->instantiate_adapters<string_name, json_name>();
    //auto thing = psc->create_map_adapter<string_name, json_name>();
    //std::cout << type_name< decltype(thing)>() << std::endl;

    //auto thing =
    //auto x = thing.create_map_adapter();
    auto c = Constructor<std::string, json, Batch<json>, Batch<std::string>>();

    //auto f = c.get_map_constructor<string_name, json_name>();
    auto x = c.create_map_adapter(map_string_to_json);
    auto y = c.create_map_adapter(map_random_trig_work<json>);
    auto z = c.get_map_adapter<json_name, string_name>(map_string_to_json);

    //std::cout << type_name<decltype(f)>() << std::endl;
    std::cout << type_name<decltype(x)>() << std::endl;
    std::cout << type_name<decltype(y)>() << std::endl;

    return 0;
    tf::Executor executor(workers);

    // TODO: Apparently this can't be done inside a class
    LinearPipeline lp(executor, psc, true);

    lp.source(new FileSourceAdapter(std::string("devin_poc/without_data_len.json"), rate_per_sec))
     .filter(filter_random_drop)                // Randomly drop 50% of packets
     .map(map_string_to_json)                   // Parse strings into JSON objects
     .explode<json>(exploder_duplicate)         // Duplicated every JSON object 10x
     .map(map_random_work_on_json_object)       // Perform various operations on each JSON object
     .map<json>(map_random_trig_work)           // Do intensive trig work and forward JSON packets
     .explode<json>(exploder_duplicate)         // Duplicate every JSON object 10x
     .filter(filter_random_drop)                // Randomly drop 50% of packets
     .map<json>(map_random_trig_work)           // Do intensive trig work and forward JSON packets
     .explode<json>(exploder_duplicate)         // Duplicated every JSON object 10x
     .batch(10, 10)                             // Batch 10 JSON objects at a time and forward
     .sink(sink_discard)                        // Sink all packets
     .add_conditional_stage(map_conditional_jump_to_start) // Taskflow loopback
     .visualize("main_graph.dot")
     .start(timeout);

    std::cout << "Stopped" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));

    return 0;
}