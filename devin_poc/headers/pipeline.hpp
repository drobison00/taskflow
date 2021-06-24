//
// Created by drobison on 6/23/21.
//
#include <adapters.hpp>
#include <blockingconcurrentqueue.h>
#include <example_task_funcs.hpp>

#include <nlohmann/json.hpp>
#include <taskflow/taskflow.hpp>

#include <boost/fiber/all.hpp>


#ifndef TASKFLOW_PIPELINE_HPP
#define TASKFLOW_PIPELINE_HPP
using namespace moodycamel;
using namespace nlohmann;

namespace taskflow_pipeline {
template<typename T>
    constexpr auto type_name() noexcept {
    std::string_view name = "Error: unsupported compiler", prefix, suffix;
    #ifdef __clang__
        name = __PRETTY_FUNCTION__;
              prefix = "auto type_name() [T = ";
              suffix = "]";
    #elif defined(__GNUC__)
        name = __PRETTY_FUNCTION__;
              prefix = "constexpr auto type_name() [with T = ";
              suffix = "]";
    #elif defined(_MSC_VER)
        name = __FUNCSIG__;
              prefix = "auto __cdecl type_name<";
              suffix = ">(void) noexcept";
    #endif
    name.remove_prefix(prefix.size());
    name.remove_suffix(suffix.size());

    return name;
}

class Pipeline {
public:
    enum StageType {
        op_custom, op_source, op_sink, op_filter, op_map, op_batch, op_explode, op_conditional
    };

    enum PipelineState {
        uninitialized, initialized, running
    };

    PipelineState pstate = uninitialized;
    std::string _name = "Pipeline";

    std::atomic<unsigned int> pipeline_running = 0;

    std::map <std::string, tf::Task> task_map;
    std::map <std::string, std::shared_ptr<StageAdapterExt>> adapter_map;

    tf::Taskflow pipeline;
    tf::Executor &service_executor;

    Pipeline(tf::Executor &executor) : service_executor{executor} {
        boost::fibers::use_scheduling_algorithm<boost::fibers::algo::shared_work>();
        task_map = std::map<std::string, tf::Task>();
        adapter_map = std::map < std::string, std::shared_ptr < StageAdapterExt >> ();

        pipeline.name(_name);

        auto init = pipeline.emplace([]() {}).name("init");
        task_map[std::string("__init")] = init;

        auto shutdown = pipeline.emplace([]() {}).name("end");
        task_map[std::string("__end")] = shutdown;
    }

    Pipeline &build() {
        service_executor.run(pipeline).wait();
        pstate = initialized;

        return *this;
    }

    Pipeline &start(unsigned int runtime) {
        pipeline_running = 1;

        service_executor.run(pipeline).wait();

        std::this_thread::sleep_for(std::chrono::seconds(runtime));

        return *this;
    }

    Pipeline &name(std::string name) {
        name = name;
        pipeline.name(name);

        return *this;
    }

    Pipeline &wait() {
        return *this;
    }

    Pipeline &visualize(std::string filename) {
        std::ofstream file;
        file.open(filename);
        pipeline.dump(file);
        file.close();

        return *this;
    };

    template<typename FType>
    Pipeline &add_node(std::string &&adapter_type, std::string &&task_name, std::string &&output_type,
                       std::vector <std::string> &&inputs,
                       FType func);
};

template<typename FType>
Pipeline &Pipeline::add_node(std::string &&adapter_type,
                             std::string &&task_name,
                             std::string &&output_type,
                             std::vector <std::string> &&inputs,
                             FType func) {
    auto task_it = task_map.find(task_name);
    if (task_it != task_map.end()) {
        std::stringstream sstream;
        sstream << "Task ID: " << task_name << " already exists.";
        throw (sstream.str());
    }

    std::shared_ptr<StageAdapterExt> adapter;
    if (adapter_type == "file") {
        adapter = std::shared_ptr<StageAdapterExt>(new FileSourceAdapterExt());
    } else if (adapter_type == "map") {
        adapter = std::shared_ptr<StageAdapterExt>(new MapAdapterExt());
    } else {
        adapter = std::shared_ptr<StageAdapterExt>(new StageAdapterExt());
    }

    adapter->output_type = output_type;
    adapter_map[task_name] = adapter;

    // This won't be run until later
    auto task = pipeline.emplace([this, adapter, inputs, func]() {
        unsigned int a = 0, b = 1;

        if (this->pstate == uninitialized and adapter->initialized == 0) {
            for (auto input = inputs.begin(); input < inputs.end(); ++input) {
                auto parent = this->adapter_map.find(*input);
                if (parent == adapter_map.end()) {
                    throw ("Cycles are not currently supported");
                }

                auto edge = std::shared_ptr<BlockingConcurrentQueue<EdgeData>>(
                        new BlockingConcurrentQueue<EdgeData>());
                //std::cout << "-------" << std::endl;
                //std::cout << type_name< decltype(edge)>() << std::endl;
                //std::cout << "-------" << std::endl;

                parent->second->subscribers.push_back(edge);
                adapter->inputs.push_back(edge);
            }
            adapter->initialized = 1;
        } else if (this->pstate == initialized) {
            if (adapter->running.compare_exchange_strong(a, b)) {
                adapter->init();

                std::thread([this, adapter, func]() {
                    boost::fibers::use_scheduling_algorithm<boost::fibers::algo::shared_work>();
                    while (this->pipeline_running == 1) {
                        adapter->pump();
                    }
                    adapter->running = 0;
                }).detach();
            }
        }
    }).name(task_name);

    if (inputs.size() == 0) {
        auto parent = this->task_map.find("__init");
        auto ptask = parent->second;

        ptask.precede(task);
    }

    if (output_type == "none") {
        auto end = this->task_map.find("__end");
        auto etask = end->second;

        task.precede(etask);
    }

    for (auto input = inputs.begin(); input < inputs.end(); ++input) {
        auto parent = this->task_map.find(*input);
        if (parent == task_map.end()) {
            throw ("Cycles are not currently supported");
        }

        auto ptask = parent->second;
        ptask.precede(task);
    }

    task_map[task_name] = task;

    return *this;
};

}

#endif //TASKFLOW_PIPELINE_HPP
