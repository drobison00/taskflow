//
// Created by drobison on 6/22/21.
//
#include <nlohmann/json.hpp>
#include <common.hpp>

#ifndef TASKFLOW_EXAMPLE_TASK_FUNCS_HPP
#define TASKFLOW_EXAMPLE_TASK_FUNCS_HPP
using namespace nlohmann;

PrimitiveVariant map_parse_to_json(const PrimitiveVariant v);
PrimitiveVariant map_json_to_json(const PrimitiveVariant v);
PrimitiveVariant map_random_work_on_json_object(const PrimitiveVariant v);
PrimitiveVariant map_random_trig_work_and_forward(const PrimitiveVariant v);
std::vector<PrimitiveVariant> flat_map_duplicate(const PrimitiveVariant v);
PrimitiveVariant multi_map_merge_json(const std::vector<PrimitiveVariant> v);
void sink_passthrough(const PrimitiveVariant v);



// Take a single element vector containing a string, and parse to a json object
PrimitiveVariant map_parse_to_json(const PrimitiveVariant v)  {
    std::shared_ptr<std::string> s = boost::get<std::shared_ptr<std::string>>(v);
    //std::cout << "Parsing: " << *s << std::endl;
    return std::shared_ptr<json>(new json(json::parse(*s)));
}

PrimitiveVariant map_json_to_json(const PrimitiveVariant v) {
    return v;
}

PrimitiveVariant map_random_work_on_json_object(const PrimitiveVariant v) {
    auto j_sp = boost::get<std::shared_ptr<json>>(v);
    json *_j = new json(*j_sp);
    json &j = *_j;

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

    return std::shared_ptr<json>(_j);
}

PrimitiveVariant map_random_trig_work_and_forward(const PrimitiveVariant v) {
    double MYPI = 3.14159265;
    int how_many = std::rand() % 1000;
    double random_angle_rads = MYPI * ((double) std::rand() / (double) RAND_MAX);

    for (int i = 0; i < how_many;) {
        random_angle_rads = tan(atan(random_angle_rads));

        // Don't let GCC optimize us away
        __asm__ __volatile__("inc %[Incr]" : [Incr] "+r"(i));
    }

    return v;
}

std::vector<PrimitiveVariant> flat_map_duplicate(const PrimitiveVariant v) {
    std::vector<PrimitiveVariant> ret;

    for(int i = 0; i < 10; i++){
        ret.push_back(v);
    }

    return ret;
}

PrimitiveVariant multi_map_merge_json(const std::vector<PrimitiveVariant> v) {
    std::shared_ptr<json> j = boost::get<std::shared_ptr<json>>(v[0]);
    json *merged = new json(*j);
    for (auto it=v.begin() + 1; it != v.end(); it++) {
        std::shared_ptr<json> j = boost::get<std::shared_ptr<json>>(*it);
        merged->merge_patch(*j);
    }

    return std::shared_ptr<json>(merged);
}

void sink_passthrough(const PrimitiveVariant v) {}

bool filter_passthrough(const PrimitiveVariant v) {
    return true;
}

bool filter_random_drop(const PrimitiveVariant v) {

    if (std::rand() % 2 == 0){
        return true;
    }
    return false;
}

#endif //TASKFLOW_EXAMPLE_TASK_FUNCS_HPP
