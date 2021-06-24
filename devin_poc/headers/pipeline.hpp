//
// Created by drobison on 6/23/21.
//
#include <pipeline_adapters.hpp>
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

        Pipeline &add_node(std::shared_ptr <StageAdapterExt> adapter);

        Pipeline &source(std::string &&source_type, std::string &&connection_string,
                                   std::string &&task_name);

        Pipeline &sink(std::string &&task_name, std::vector <std::string> &&inputs,
                                 void(*func)(std::vector <DataVariant>));

        Pipeline &map(std::string &&task_name, std::vector <std::string> &&inputs,
                                DataVariant(*func)(std::vector <DataVariant>));
    private:
        void check_assert_task_exists(std::string task_name) {
            auto task_it = task_map.find(task_name);
            if (task_it != task_map.end()) {
                std::stringstream sstream;
                sstream << "Task ID: " << task_name << " already exists.";
                throw (sstream.str());
            }
        }
    };

    Pipeline &Pipeline::source(std::string &&source_type,
                               std::string &&connection_string,
                               std::string &&task_name) {

        check_assert_task_exists(task_name);
        std::shared_ptr <StageAdapterExt> adapter;
        if (source_type == "file") {
            adapter = std::shared_ptr<StageAdapterExt>(
                    new FileSourceAdapterExt(connection_string));
        } else {
            throw ("Unknown Source Adapter Type");
        }

        adapter->name = task_name;
        adapter->type_id = StageType::op_source;

        return add_node(adapter);
    }

    Pipeline &Pipeline::sink(std::string &&task_name,
                            std::vector <std::string> &&inputs,
                            void(*func)(std::vector <DataVariant>)) {

        check_assert_task_exists(task_name);

        auto adapter = std::shared_ptr<StageAdapterExt>(new SinkAdapterExt(func));
        adapter->name = task_name;
        adapter->input_names = inputs;
        adapter->type_id = StageType::op_sink;

        return add_node(adapter);
    }

    Pipeline &Pipeline::map(std::string &&task_name,
                            std::vector <std::string> &&inputs,
                            DataVariant(*func)(std::vector <DataVariant>)) {

        check_assert_task_exists(task_name);

        auto adapter = std::shared_ptr<StageAdapterExt>(new MapAdapterExt(func));
        adapter->name = task_name;
        adapter->input_names = inputs;
        adapter->type_id = StageType::op_map;

        return add_node(adapter);
    }

    Pipeline &Pipeline::add_node(std::shared_ptr <StageAdapterExt> adapter) {

        check_assert_task_exists(adapter->name);
        adapter_map[adapter->name] = adapter;

        // This won't be run until later
        auto task = pipeline.emplace([this, adapter]() {
            unsigned int a = 0, b = 1;

            if (this->pstate == uninitialized and adapter->initialized == 0) {
                for (auto input = adapter->input_names.begin(); input < adapter->input_names.end(); ++input) {
                    auto parent = this->adapter_map.find(*input);
                    if (parent == adapter_map.end()) {
                        std::stringstream sstream;
                        sstream << "Input ID: "
                                << *input << " does not exist at this point in graph construction "
                                << " Note: cycles are not currently supported.\n";
                        throw (sstream.str());
                    }
                    auto padapter = parent->second;

                    auto edge = std::shared_ptr < BlockingConcurrentQueue < DataVariant >> (
                            new BlockingConcurrentQueue<DataVariant>());

                    padapter->subscriber_names.push_back(*input);
                    padapter->subscribers.push_back(edge);
                    adapter->inputs.push_back(edge);
                }
                adapter->initialized = 1;
            } else if (this->pstate == initialized) {
                if (adapter->running.compare_exchange_strong(a, b)) {
                    adapter->init();

                    std::thread([this, adapter]() {
                        boost::fibers::use_scheduling_algorithm<boost::fibers::algo::shared_work>();
                        while (this->pipeline_running == 1) {
                            adapter->pump();
                        }
                        adapter->running = 0;
                    }).detach();
                }
            }
        }).name(adapter->name);

        if (adapter->input_names.size() == 0) {
            auto parent = this->task_map.find("__init");
            auto ptask = parent->second;

            ptask.precede(task);
        }

        if (adapter->type_id == StageType::op_sink) {
            auto end = this->task_map.find("__end");
            auto etask = end->second;

            task.precede(etask);
        }

        for (auto input = adapter->input_names.begin(); input < adapter->input_names.end(); ++input) {
            auto parent = this->task_map.find(*input);
            if (parent == task_map.end()) {
                throw ("Cycles are not currently supported");
            }

            auto ptask = parent->second;
            ptask.precede(task);
        }

        task_map[adapter->name] = task;

        return *this;
    };

}

#endif //TASKFLOW_PIPELINE_HPP
