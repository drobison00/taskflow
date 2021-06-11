//
// Created by drobison on 6/9/21.
//

#include <blockingconcurrentqueue.h>

#include <nlohmann/json.hpp>
#include <taskflow/taskflow.hpp>

#ifndef TASKFLOW_BUILDING_BLOCKS_HPP
#define TASKFLOW_BUILDING_BLOCKS_HPP
using namespace moodycamel;
using namespace nlohmann;

template<class SourceType, class OutputType>
class SourceAdapter {
public:
    std::string connection_string;
    std::string data_debug;

    unsigned int processed;
    unsigned int max_read_rate;

    using OutputQueue = std::shared_ptr<BlockingConcurrentQueue<OutputType>>;

    SourceType source;
    OutputQueue output;

    SourceAdapter(std::string connection_string, unsigned int max_read_rate) :
            connection_string{connection_string}, max_read_rate{max_read_rate}, processed{0} {
        output = OutputQueue(new BlockingConcurrentQueue <OutputType>());
    };

    void init();

    void pump();

    OutputQueue get_output_edge() {
        return output;
    }

    unsigned int queue_size() {
        return output->size_approx();
    }
};

// Specialize for specific input/output cases
template<>
void SourceAdapter<std::fstream, std::string>::pump() {
    //std::string data;
    //std::getline(source, data);
    output->enqueue(data_debug);
    processed += 1;
};

template<>
void SourceAdapter<std::fstream, std::string>::init() {
    source = std::fstream();
    source.open(connection_string);
    std::getline(source, data_debug);
};

typedef SourceAdapter<std::fstream, std::string> FileSourceAdapter;

template<class InputType>
class SinkThrowAway {
public:
    void pump(InputType) {};
};

template<class InputType, class SinkType>
class SinkAdapter {
public:
    using InputQueue = std::shared_ptr<BlockingConcurrentQueue<InputType>>;

    std::string connection_string;

    unsigned int processed;

    SinkType sink;
    InputQueue input;

    SinkAdapter(std::string connection_string, InputQueue input) :
            connection_string{connection_string}, input{input}, processed{0} {};

    void init();

    void pump();

    unsigned int queue_size() {
        return input->size_approx();
    }
};

// Specialize for specific input/output cases
template<class InputType, class SinkType>
void SinkAdapter<InputType, SinkType>::pump() {
    size_t count;
    InputType in[1];

    count = input->wait_dequeue_bulk_timed(&in[0], 1, std::chrono::milliseconds(10));
    if (count > 0) {
        sink.pump(in[0]);
        processed += 1;
    }
};

template<class InputType, class SinkType>
void SinkAdapter<InputType, SinkType>::init() {};

template<class InputType, class OutputType>
class StageAdapter {
public:
    using InputQueue = std::shared_ptr<BlockingConcurrentQueue<InputType>>;
    using OutputQueue = std::shared_ptr<BlockingConcurrentQueue<OutputType>>;

    unsigned int processed = 0;

    std::function<OutputType(InputType)> work_routine;

    InputQueue input;
    OutputQueue output;

    StageAdapter(std::function<OutputType(InputType)> work_routine, InputQueue input) :
            work_routine{work_routine}, input{input} {
        output = OutputQueue(new BlockingConcurrentQueue<OutputType>());
    };

    int pump();

    OutputQueue get_output_edge() {
        return output;
    }

    unsigned int queue_size() {
        return input->size_approx();
    }
};

template<class InputType, class OutputType>
int StageAdapter<InputType, OutputType>::pump() {
    size_t count = 0;
    InputType in[1];
    OutputType out;

    count = input->wait_dequeue_bulk_timed(&in[0], 1, std::chrono::milliseconds(10));
    if (count > 0) {
        out = work_routine(in[0]);
        output->enqueue(out);
        processed += 1;
    }

    return processed;
};

// Example worker task
json work_routine_string_to_json(std::string s) {
    return json::parse(s);
}

json work_routine_random_work_on_json_object(json j) {
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

    return j;
}

struct TaskStats {
    unsigned int stage_type;

    unsigned int last_processed = 0 ;
    unsigned int last_queue_size = 0;

    double avg_throughput = 0.0;
    double avg_delta_throughput = 0.0;
    double avg_queue_size = 0.0;
    double avg_delta_queue_size = 0.0;
    double avg_time_between_visits = 0.0;

    std::chrono::time_point<std::chrono::steady_clock> last_visited;
};

class LinearPipeline {
    enum StageType { source, intermediate, sink, conditional };
public:
    unsigned int stages = 0;
    bool print_stats = false;

    std::atomic<unsigned int> pipeline_running = 0;
    std::vector<std::atomic < unsigned int>*> stage_running_flags;
    std::vector<std::shared_ptr<void>> stage_adapters;
    std::vector<std::shared_ptr<void>> edges;
    std::vector<std::unique_ptr<TaskStats>> task_stats;
    std::map<unsigned int, tf::Task> id_to_task_map;
    std::vector <tf::Task> task_chain;

    tf::Task init;
    tf::Taskflow pipeline;
    tf::Executor &service_executor;

    ~LinearPipeline() {
    };

    LinearPipeline(tf::Executor &executor) : service_executor{executor} {
        init = pipeline.emplace([this]() {
            std::cout << "Initializing Pipeline" << std::endl;
        });
    }

    LinearPipeline(tf::Executor &executor, bool print_stats) :
        service_executor{executor}, print_stats{print_stats} {
        init = pipeline.emplace([this]() {
            std::cout << "Initializing Pipeline" << std::endl;

            if (this->print_stats) {
                service_executor.async([this](){
                    unsigned int freq_ms = 33;
                    std::stringstream sstream;

                    auto start_time = std::chrono::steady_clock::now();
                    while (this->pipeline_running != 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(freq_ms));
                        auto cur_time = std::chrono::steady_clock::now();
                        std::chrono::duration<double> elapsed = cur_time - start_time;

                        sstream.str("");
                        sstream << "Throughput msg/sec: ";
                        for (auto i = 0; i < this->task_stats.size(); i++) {
                            switch (this->task_stats[i]->stage_type) {
                                case StageType::conditional:
                                {
                                    sstream << "[cond], ";
                                    break;
                                }
                                case StageType::source:
                                {
                                    sstream << "[pumped]" << " => "
                                            << std::setw(10) << std::setprecision(0) << std::fixed <<
                                            this->task_stats[i]->avg_throughput << "(" <<
                                            this->task_stats[i]->avg_queue_size << "), ";
                                    break;
                                }

                                default: {
                                    sstream << "[q." << i << "] => "
                                            << std::setw(8) << std::setprecision(0) << std::fixed <<
                                            this->task_stats[i]->avg_throughput << ", ";
                                }
                            }

                        }
                        sstream << " runtime: " << std::setw(8) << elapsed.count() << " sec\r";
                        std::cout << sstream.str() << std::flush;
                    }
                });
            }
        });
    }

    void start() {
        pipeline_running = 1;
        service_executor.run(pipeline).wait();
    }

    void stop() {
        pipeline_running = 0;
    }

    template<class InputType, class OutputType>
    void add_stage(std::function<OutputType(InputType)> work_routine);

    void add_conditional_stage(unsigned int (*cond_test)(LinearPipeline *));

    template<class SourceType, class OutputType>
    void set_source(std::string connection_string, unsigned int max_read_rate);

    template<class InputType, class SinkType>
    void set_sink(std::string connection_string);

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

        auto avg_throughput = 0.99 * last_avg_throughput + 0.01 * delta_proc / delta_visit.count();
        auto avg_queue_size = 0.99 * last_avg_queue_size + 0.01 * queue_size;

        auto delta_avg_throughput = last_avg_throughput - avg_throughput;
        auto delta_avg_queue_size = last_avg_queue_size - avg_queue_size;

        auto avg_delta_throughput = 0.99 * last_avg_throughput + 0.01 * delta_avg_throughput;
        auto avg_delta_queue_size = 0.99 * last_avg_queue_size + 0.01 * delta_avg_queue_size;


        task_stats[index]->last_visited = std::chrono::steady_clock::now();
        task_stats[index]->last_processed = processed;
        task_stats[index]->last_queue_size = queue_size;

        task_stats[index]->avg_throughput = avg_throughput;
        task_stats[index]->avg_delta_throughput = avg_delta_throughput;
        task_stats[index]->avg_queue_size = avg_queue_size;
        task_stats[index]->avg_delta_queue_size = avg_delta_queue_size;

        task_stats[index]->avg_time_between_visits =
                0.99 * task_stats[index]->avg_time_between_visits + 0.01 * delta_visit.count();

    }
};

void LinearPipeline::add_conditional_stage(unsigned int (*cond_test)(LinearPipeline *)) {
    // TODO: this is entirely ad-hoc right now. It exists only as a way to add the jump to start
    // conditional. Still thinking through the architecture requirements to generalize.
    std::atomic<unsigned int> *run_flag = new std::atomic<unsigned int>(0);
    auto index = stage_running_flags.size();
    stage_running_flags.push_back(run_flag);


    task_stats.push_back(std::unique_ptr<TaskStats>(new TaskStats()));
    task_stats[index]->stage_type = StageType::conditional;
    std::cout << "Adding conditional at " << index << std::endl;

    tf::Task conditional = pipeline.emplace([this, cond_test]() {
        return cond_test(this);
    });

    id_to_task_map[index] = conditional;
    task_chain.push_back(conditional);
    task_chain[index - 1].precede(conditional);
    conditional.precede(task_chain[0], task_chain[index - 1]);
}

template<class InputType, class OutputType>
void LinearPipeline::add_stage(std::function<OutputType(InputType)> work_routine) {
    std::atomic<unsigned int> *run_flag = new std::atomic<unsigned int>(0);
    auto index = stage_running_flags.size();
    stage_running_flags.push_back(run_flag);

    std::cout << "Adding stage " << index << std::endl;
    std::shared_ptr<BlockingConcurrentQueue <InputType>> input =
            std::static_pointer_cast<BlockingConcurrentQueue <InputType>>(edges[index - 1]);

    auto adapter = std::shared_ptr<StageAdapter<InputType, OutputType>>(
            new StageAdapter<InputType, OutputType>(work_routine, input));

    stage_adapters.push_back(adapter);
    edges.push_back(adapter->get_output_edge());
    task_stats.push_back(std::unique_ptr<TaskStats>(new TaskStats()));
    task_stats[index]->last_visited = std::chrono::steady_clock::now();
    task_stats[index]->stage_type = StageType::intermediate;

    tf::Task stage_task = pipeline.emplace([this, index, adapter]() {
        unsigned int a = 0, b = 1; // TODO should be class level enums

        if ((this->stage_running_flags[index])->compare_exchange_strong(a, b)) {
            std::cout << "Initializing stage " << index << " task." << std::endl;
            service_executor.async([&]() {
                while (this->pipeline_running == 1) {
                    adapter->pump();
                }
                this->stage_running_flags[index]->compare_exchange_strong(b, a);
            });
        }

        update_task_stats(index, adapter->processed, adapter->queue_size());
    });

    id_to_task_map[index] = stage_task;
    task_chain.push_back(stage_task);
    task_chain[index - 1].precede(stage_task);
}

template<class SourceType, class OutputType>
void LinearPipeline::set_source(std::string connection_string, unsigned int max_read_rate) {
    std::atomic<unsigned int> *run_flag = new std::atomic<unsigned int>(0);
    auto index = stage_running_flags.size();
    stage_running_flags.push_back(run_flag);

    auto adapter = std::shared_ptr<SourceAdapter<SourceType, OutputType>>(
            new SourceAdapter<SourceType, OutputType>(connection_string, max_read_rate));

    stage_adapters.push_back(adapter);
    edges.push_back(adapter->get_output_edge());
    task_stats.push_back(std::unique_ptr<TaskStats>(new TaskStats()));
    task_stats[index]->last_visited = std::chrono::steady_clock::now();
    task_stats[index]->stage_type = StageType::source;

    std::cout << "Adding pipeline source" << std::endl;
    tf::Task source_task = pipeline.emplace([this, adapter, index]() {
        unsigned int a = 0, b = 1;

        if ((this->stage_running_flags[index])->compare_exchange_strong(a, b)) {
            std::cout << "Initializing source task." << std::endl;
            adapter->init();

            service_executor.async([&]() {
                while (this->pipeline_running == 1) {
                    if (adapter->processed > adapter->max_read_rate) {
                        if (this->task_stats[index]->avg_queue_size < adapter->max_read_rate) {
                            adapter->pump();
                        }
                    } else {
                        adapter->pump();
                    }
                }
                this->stage_running_flags[index]->compare_exchange_strong(b, a);
            });
        }

        update_task_stats(index, adapter->processed, adapter->queue_size());
    }).name("source");

    id_to_task_map[index] = source_task;
    task_chain.push_back(source_task);
    init.precede(source_task);
};

template<class InputType, class SinkType>
void LinearPipeline::set_sink(std::string connection_string) {
    std::atomic<unsigned int> *run_flag = new std::atomic<unsigned int>(0);
    auto index = stage_running_flags.size();
    stage_running_flags.push_back(run_flag);

    std::shared_ptr<BlockingConcurrentQueue <InputType>> input =
            std::static_pointer_cast<BlockingConcurrentQueue <InputType>>(edges[index - 1]);

    auto adapter = std::shared_ptr<SinkAdapter<InputType, SinkType>>(
            new SinkAdapter<InputType, SinkType>(connection_string, input));

    stage_adapters.push_back(adapter);
    task_stats.push_back(std::unique_ptr<TaskStats>(new TaskStats()));
    task_stats[index]->last_visited = std::chrono::steady_clock::now();
    task_stats[index]->stage_type = StageType::sink;

    std::cout << "Adding pipeline sink" << std::endl;
    tf::Task sink_task = pipeline.emplace([this, adapter, index]() {
        unsigned int a = 0, b = 1;

        if ((this->stage_running_flags[index])->compare_exchange_strong(a, b)) {
            std::cout << "Initializing sink task." << std::endl;
            adapter->init();

            service_executor.async([&]() {
                while (this->pipeline_running == 1) { adapter->pump(); }
                this->stage_running_flags[index]->compare_exchange_strong(b, a);
            });
        }

        update_task_stats(index, adapter->processed, adapter->queue_size());
        /*
        if (sleep_time > 0) {
            std::cout << "[Business Logic] Data sink has cleared " << adapter->processed
                      << " tasks" << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
        }
        sleep_time = 1;
        */
    }).name("sink");

    id_to_task_map[index] = sink_task;
    task_chain.push_back(sink_task);
    task_chain[index - 1].precede(sink_task);
};

unsigned int work_routine_conditional_jump_to_start(LinearPipeline *lp) {
    return 0;
}

#endif //TASKFLOW_BUILDING_BLOCKS_HPP
