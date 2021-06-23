//
// Created by drobison on 6/9/21.
//
#include <adapters.hpp>
#include <blockingconcurrentqueue.h>
#include <example_task_funcs.hpp>

#include <nlohmann/json.hpp>
#include <taskflow/taskflow.hpp>

#include <boost/fiber/all.hpp>

#ifndef TASKFLOW_BUILDING_BLOCKS_HPP
#define TASKFLOW_BUILDING_BLOCKS_HPP
using namespace moodycamel;
using namespace nlohmann;

template<typename T>
constexpr auto type_name()

noexcept {
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
};

class LinearPipeline;

struct TaskStats {
    unsigned int stage_type;

    unsigned int last_processed = 0;
    unsigned int last_queue_size = 0;

    double avg_throughput = 0.0;
    double avg_delta_throughput = 0.0;
    double avg_queue_size = 0.0;
    double avg_delta_queue_size = 0.0;
    double avg_time_between_visits = 0.0;

    std::chrono::time_point <std::chrono::steady_clock> last_visited;
};

class LinearPipeline {
public:
    enum StageType {
        op_custom, op_source, op_sink, op_filter, op_map, op_batch, op_explode, op_conditional
    };

    unsigned int stages = 0;
    bool print_stats = false;

    // TODO
    json blueprint = {
        {
        "pipeline", {
                    {"type", "linear"},
                    {"stages", json::array()}
            }
        }
    };

    std::atomic<unsigned int> pipeline_running = 0;
    std::vector <std::shared_ptr<void>> edges;
    std::vector <std::unique_ptr<TaskStats>> task_stats;
    std::map<unsigned int, tf::Task> id_to_task_map;
    std::vector <tf::Task> task_chain;

    tf::Task init, start_task, end;
    tf::Taskflow pipeline;
    tf::Executor &service_executor;

    ~LinearPipeline() {};

    LinearPipeline(tf::Executor &executor) :
            service_executor{executor} {

        init = pipeline.emplace([]() {
        });

        end = pipeline.emplace([]() {
        });
    }

    LinearPipeline(tf::Executor &executor, bool print_stats) :
            service_executor{executor}, print_stats{print_stats} {

        init = pipeline.emplace([this]() {
            //std::cout << "Initializing Pipeline" << std::endl;

            if (this->print_stats) {
                service_executor.silent_async([this]() {
                    unsigned int freq_ms = 33;
                    std::stringstream sstream;

                    auto start_time = std::chrono::steady_clock::now();
                    while (this->pipeline_running != 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(freq_ms));
                        auto cur_time = std::chrono::steady_clock::now();
                        std::chrono::duration<double> elapsed = cur_time - start_time;

                        sstream.str("");
                        for (auto i = 0; i < this->task_stats.size(); i++) {
                            switch (this->task_stats[i]->stage_type) {
                                case StageType::op_conditional: {
                                    sstream << "[cond], ";
                                }
                                    break;
                                case StageType::op_source: {
                                    sstream << "[sourced]" << " => ";
                                }
                                    break;
                                case StageType::op_sink: {
                                    sstream << "[sunk]" << " => ";
                                }
                                    break;
                                case StageType::op_map: {
                                    sstream << "[map]" << " => ";
                                }
                                    break;
                                case StageType::op_filter: {
                                    sstream << "[filter]" << " => ";
                                }
                                    break;
                                case StageType::op_explode: {
                                    sstream << "[explode]" << " => ";
                                }
                                    break;
                                case StageType::op_batch: {
                                    sstream << "[batch]" << " => ";
                                }
                                    break;
                            }
                            sstream << std::setw(6) << std::setprecision(0) << std::fixed <<
                                    this->task_stats[i]->avg_throughput << "(" <<
                                    std::setw(4) << std::setprecision(0) << std::fixed <<
                                    this->task_stats[i]->avg_queue_size << "), ";
                        }

                        sstream << " runtime: " << std::setw(4) << elapsed.count() << " sec\r";
                        std::cout << sstream.str() << std::flush;
                    }
                });
            }
        });

        end = pipeline.emplace([]() {
            std::cout << std::endl << std::flush << "\nTerminating Pipeline" << std::endl;
        });
    }

    LinearPipeline &start(unsigned int runtime = 0) {
        pipeline_running = 1;
        boost::fibers::use_scheduling_algorithm< boost::fibers::algo::shared_work >();

        if (runtime > 0) {
            service_executor.silent_async([this, runtime]() {
                std::this_thread::sleep_for(std::chrono::seconds(runtime));
                this->stop();
            });
        }

        service_executor.run(pipeline).wait();

        return *this;
    }

    LinearPipeline &stop() {
        pipeline_running = 0;

        return *this;
    }

    LinearPipeline &visualize(std::string filename) {
        std::ofstream file;
        file.open(filename);
        pipeline.dump(file);
        file.close();

        return *this;
    }

    template<class InputType, class OutputType>
    LinearPipeline &add_stage(
            std::shared_ptr<StageAdapter<InputType, OutputType>> adapter, StageType type);

    template<class InputType, class OutputType>
    LinearPipeline &add_stage(StageAdapter<InputType, OutputType> *adapter, StageType type);

    template<class OutputType>
    LinearPipeline &source(StageAdapter<void, OutputType> *adapter) {
        return add_stage(adapter, StageType::op_source);
    }

    LinearPipeline &source(std::string&& source_type, std::string&& connection_type) {
        if (source_type == "file") {
            auto adapter = new FileSourceAdapter(connection_type);
            return add_stage(adapter, StageType::op_source);
        } else {
            throw ("Unsupported source type.");
        }
    }

    template<class InputType>
    LinearPipeline &sink(void(*sink)(InputType *)) {
        return add_stage(new DiscardSinkAdapter<InputType>(std::string(""), sink), StageType::op_sink);
    }

    template<class InputType, class OutputType>
    LinearPipeline &map(OutputType *(*map)(InputType *)) {
        return add_stage(new MapAdapter<InputType, OutputType>(std::function<OutputType *(InputType *)>(map)),
                         StageType::op_map);
    };

    template<class DataType>
    LinearPipeline &filter(bool(*filter)(DataType *)) {
        return add_stage(new FilterAdapter<DataType>(std::function<bool(DataType *)>(filter)),
                         StageType::op_filter);
    };

    template<class DataType>
    LinearPipeline &batch(
            unsigned int batch_size, unsigned int timeout_ms) {
        return add_stage(new BatchAdapter<DataType>(batch_size, timeout_ms),
                         StageType::op_batch);
    };

    template<class InputType, class OutputType>
    LinearPipeline &explode(
            std::tuple<OutputType **, unsigned int>(*exploder)(InputType *)) {
        return add_stage(new ExplodeAdapter<InputType, OutputType>(exploder),
                         StageType::op_explode);
    };

    LinearPipeline &add_conditional_stage(unsigned int (*cond_test)(LinearPipeline *));


    void update_task_stats(unsigned int index, unsigned int processed, unsigned int queue_size) {
        auto cur_time = std::chrono::steady_clock::now();
        auto last_time = task_stats[index]->last_visited;
        auto last_processed = task_stats[index]->last_processed;
        auto last_queue_size = task_stats[index]->last_queue_size;
        auto last_avg_throughput = task_stats[index]->avg_throughput;
        auto last_avg_queue_size = task_stats[index]->avg_queue_size;

        auto delta_proc = processed - last_processed;
        auto delta_queue_size = last_queue_size - queue_size;
        std::chrono::duration<double> delta_visit = cur_time - last_time;

        auto weight = 0.99999;
        auto avg_throughput = weight * last_avg_throughput + (1 - weight) * delta_proc / delta_visit.count();
        auto avg_queue_size = weight * last_avg_queue_size + (1 - weight) * queue_size;

        auto delta_avg_throughput = last_avg_throughput - avg_throughput;
        auto delta_avg_queue_size = last_avg_queue_size - avg_queue_size;

        auto avg_delta_throughput = weight * last_avg_throughput + (1 - weight) * delta_avg_throughput;
        auto avg_delta_queue_size = weight * last_avg_queue_size + (1 - weight) * delta_avg_queue_size;


        task_stats[index]->last_visited = std::chrono::steady_clock::now();
        task_stats[index]->last_processed = processed;
        task_stats[index]->last_queue_size = queue_size;

        task_stats[index]->avg_throughput = avg_throughput;
        task_stats[index]->avg_delta_throughput = avg_delta_throughput;
        task_stats[index]->avg_queue_size = avg_queue_size;
        task_stats[index]->avg_delta_queue_size = avg_delta_queue_size;

        task_stats[index]->avg_time_between_visits =
                weight * task_stats[index]->avg_time_between_visits + (1 - weight) * delta_visit.count();
    }
};

LinearPipeline &LinearPipeline::add_conditional_stage(unsigned int (*cond_test)(LinearPipeline *)) {
    // TODO: this is entirely ad-hoc right now. It exists only as a way to add the jump to start
    // conditional. Still thinking through the architecture requirements to generalize.
    auto index = stages++;
    task_stats.push_back(std::unique_ptr<TaskStats>(new TaskStats()));
    task_stats[index]->stage_type = StageType::op_conditional;

    tf::Task conditional = pipeline.emplace([this, cond_test]() {
        return cond_test(this);
    });

    id_to_task_map[index] = conditional;
    task_chain.push_back(conditional);
    task_chain[index - 1].precede(conditional);
    conditional.precede(start_task, this->end);

    return *this;
}

template<class InputType, class OutputType>
LinearPipeline& LinearPipeline::add_stage(StageAdapter<InputType, OutputType>* adapter, StageType type) {
    auto index = stages++;

    json stage = json::object({
                                      {"index",       index},
                                      {"input_type",  type_name<InputType>()},
                                      {"output_type", type_name<OutputType>()}
                              });
    blueprint["pipeline"]["stages"].push_back(stage);
    //std::cout << stage.dump(4) << std::endl;

    if (type != StageType::op_source) {
        std::shared_ptr < BlockingConcurrentQueue < std::shared_ptr < InputType>>> input =
                std::static_pointer_cast <BlockingConcurrentQueue <std::shared_ptr <InputType>>> (
                        edges[index - 1]);

        adapter->input = input;
    }
    edges.push_back(adapter->get_output_edge());

    task_stats.push_back(std::unique_ptr<TaskStats>(new TaskStats()));
    task_stats[index]->last_visited = std::chrono::steady_clock::now();
    task_stats[index]->stage_type = type;

    tf::Task stage_task = pipeline.emplace([this, index, adapter]() {
        unsigned int a = 0, b = 1;

        if (adapter->running.compare_exchange_strong(a, b)) {
            adapter->init();

            std::thread([this, index, &adapter]() {
                boost::fibers::use_scheduling_algorithm< boost::fibers::algo::shared_work >();
                while (this->pipeline_running == 1) {
                    adapter->pump();
                }
                adapter->running = 0;
            }).detach();
        }

        update_task_stats(index, adapter->processed, adapter->queue_size());
    });

    id_to_task_map[index] = stage_task;
    task_chain.push_back(stage_task);

    if (type == StageType::op_source) {
        init.precede(stage_task);
        start_task = stage_task;
    } else {
        task_chain[index - 1].precede(stage_task);
    }

    return *this;
}

/*template<class InputType, class OutputType>
LinearPipeline& LinearPipeline::add_stage(StageAdapter<InputType,
        OutputType> *stage_adapter, StageType type) {
    auto adapter = std::shared_ptr<StageAdapter<InputType, OutputType>>(stage_adapter);
    return add_stage(adapter, type);
}*/


unsigned int map_conditional_jump_to_start(LinearPipeline *lp) {
    if (lp->pipeline_running) {
        return 0;
    }

    return 1;
}

#endif //TASKFLOW_BUILDING_BLOCKS_HPP
