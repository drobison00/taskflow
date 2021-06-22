//
// Created by drobison on 6/21/21.
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
#include <lookup_tables.hpp>

// Function that handles multiple types
template<typename In, typename Out>
Out* map_dynamic(In*) {
    return new Out();
}

// Function with fixed input/output types
json *map_fixed(std::string *s) {
    return new json();
}

auto workers = 8;
tf::Executor executor(workers);
LinearPipeline lp(executor, true);

/* Static compile time example.
In this example, the call's to .map are provided their input and output types, or the compiler is able
 to deduce them from the function signature. This works fine, and can be constructed arbitrarily if
 an end user wants to write the c++ code to build their pipeline.
 */
lp.source(new FileSourceAdapter(std::string("devin_poc/without_data_len.json"), rate_per_sec))
    .map(map_fixed)
    .map(map_dynamic<json, unsigned int>)
    .sink(sink_discard)
    .start();

/* Dynamic run time example.
In this example the structure of the pipeline is obtained via serialized user input. At compile time
 the compiler doesn't know what it will have to create, and its only able to create things that are
 compiled and linked in the symbol table. This results in two complications: 1) how do we determine and
 select types based on serialized user input, 2) how do we ensure that whatever the user has specified
 will be available in our symbol table.

 Suppose that we're provided the following psuedo serialization on stdin:
    "[stage_id:source] [source_type:file] [source_info:'devin_poc/without_data_len.json'] [source_rate: 10]; \
     [stage_id:map] [stage_func:map_fixed]; \
     [stage_id:map] [stage_func:map_dynamic] [stage_input_type:json] [stage_output_type:'unsigned int'];
     [stage_id:sink] [sink_func: sink_discard]"
 */

// Parse input, making sure we've got a defined source, and sink, and some number of stages

/*
 * Pseudo code for what the source add function will need to do:
 * Source is fairly easy here, we know a file source reads a file, and source_info will contain
    the path to the file
*/
LinearPipeLine &LinearPipeline::lp.add_source_from_string(
        std::string source_type, std::string source_info, std::string source_rate) {
    if (source_type == "file"){
        lp.source(new FileSourceAdapter(std::string(source_info), rate)
    } else if (source_type == "grpc") {
        // do something else
    } else if (...) {
        // ...
    } else {
        throw("Unknown source type\n");
    }
    return *this;
}
lp.add_source_from_string("source", "file", 10);

/*
 * Psuedo code for adding intermediate stages:
 *
 * In the case of adding 'map_fixed', our problems aren't too bad, the input and output types are fixed
 *  so once we have identified the function the adapter types can be inferred.
 *
 * In the case of adding 'map_dynamic', we have a much more complicated situation: we need to look up the
 *  specific instantiation of the function map_dynamic<json, 'unsigned int'> and pass it to the stage
 *  creator.
 */

template<typename In, typename Out>
decltype(auto) get_function(std::string stage_func) {
   // This part is not that clear, we need to be able to look up a templated function based on its name
   // Maybe we store void pointers?

   return std::function<In*(Out*)>(lookup_function(stage_func));
}

template<typename In>
decltype(auto) get_partial(std::string stage_func, std::string stage_output_type) {
    if (stage_input_type == "string") {
        return get_partial<std::string>(stage_func, stage_output_type);
    } else if (stage_input_type == "json") {
        return get_partial<json>(stage_func, stage_output_type);
    } else if ( ... ) {
        // enumerate all other possible types
    } else {
        throw("Unknown input type")
    }
}

decltype(auto) get_func(std::string stage_func, std::string stage_input_type, std::string stage_output_type) {
    if (stage_input_type == "string") {
       return get_partial<std::string>(stage_func, stage_output_type);
    } else if (stage_input_type == "json") {
        return get_partial<json>(stage_func, stage_output_type);
    } else if ( ... ) {
        // enumerate all other possible types
    } else {
        throw("Unknown input type")
    }
}

lp.add_stage_from_string(stage_id, stage_func, stage_input_type, stage_output_type){
    auto func = get_func(stage_func, stage_input_type, stage_output_type); // Problem

    if (stage_id == "filter") {
        // do something
    } else if (stage_id == "map") {
        add_stage(MapAdapter(func));
    } else if ( ... )
}