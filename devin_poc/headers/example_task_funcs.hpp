//
// Created by drobison on 6/22/21.
//
#include <nlohmann/json.hpp>

#ifndef TASKFLOW_EXAMPLE_TASK_FUNCS_HPP
#define TASKFLOW_EXAMPLE_TASK_FUNCS_HPP
using namespace nlohmann;

// Example worker task
json *map_string_to_json(std::string *s) {
    return new json(json::parse(*s));
}


template<class In, class Out>
Out* map_passthrough(In *o) {
    return new Out(*o);
}

json *map_random_work_on_json_object(json *_j) {
    json *__j = new json(*_j);
    json j = *__j;


    j["some field"] = "some value";
    j["some list"] = {'a', 'b', 'c'};
    j["some dict"] = {{"thing1", 1},
                      {"thing2", 3.1411111}};
    if (j["timestamp"] > 1616381017606) {
        j["is_first"] = false;
    } else {
        j["is_first"] = true;
        j.erase("flags");
    }

    // search the string
    if (j.find("dest_port") != j.end()) {
        if (j["dest_port"] == "80") {
            j["is_http_dest"] = true;
        } else {
            j["is_http_dest"] = false;
        }
    }

    return __j;
}

// Function examples

template<typename InputType>
bool filter_random_drop(InputType *d) {
    return std::rand() % 2 == 0;
};

template<typename DataType>
DataType *map_random_trig_work(DataType *d) {
    double MYPI = 3.14159265;
    int how_many = std::rand() % 100000;
    double random_angle_rads = MYPI * ((double) std::rand() / (double) RAND_MAX);

    for (int i = 0; i < how_many;) {
        random_angle_rads = tan(atan(random_angle_rads));

        // Don't let GCC optimize us away
        __asm__ __volatile__("inc %[Incr]" : [Incr] "+r"(i));
    }

    return new DataType(*d);
}

json *map_random_trig_work_json(json *d) {
    double MYPI = 3.14159265;
    int how_many = std::rand() % 100000;
    double random_angle_rads = MYPI * ((double) std::rand() / (double) RAND_MAX);

    for (int i = 0; i < how_many;) {
        random_angle_rads = tan(atan(random_angle_rads));

        // Don't let GCC optimize us away
        __asm__ __volatile__("inc %[Incr]" : [Incr] "+r"(i));
    }

    return new json();
}

std::tuple<json **, unsigned int> exploder_duplicate_json(json *d) {
    int k = 10;
    json **out = new json *[k];

    for (int i = 0; i < k; i++) {
        out[i] = new json(*d);
    }

    return std::tuple(out, k);
}


// Kind of redundant. Experimenting on ways to reduce template args in chaining
template<typename DataType>
std::tuple<DataType **, unsigned int> exploder_duplicate(DataType *d) {
    int k = 10;
    DataType **out = new DataType *[k];

    for (int i = 0; i < k; i++) {
        out[i] = new DataType(*d);
    }

    return std::tuple(out, k);
}

template<typename InputType>
void sink_discard(InputType *) {
    return;
}
#endif //TASKFLOW_EXAMPLE_TASK_FUNCS_HPP
