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


class LinearPipeline;

template <class InputType, class OutputType>
class LinearLinkInfo;

template<class SourceType, class OutputType>
class SourceAdapter;

template<class InputType>
class SinkAdapter;

template<class InputType, class OutputType>
class StageAdapter;

template<class DataType>
class BatchObject {
public:
    // Maybe other info
    unsigned int batch_size;
    std::shared_ptr<DataType[]> batch;

    BatchObject() : batch_size{0}, batch{nullptr} {};

    BatchObject(unsigned int batch_size) : batch_size{batch_size} {
        batch = std::shared_ptr<DataType[]>(new DataType[batch_size]);
    };
};

// Source Adapters
template<class SourceType, class OutputType>
class SourceAdapter {
public:
    std::string connection_string;
    std::string data_debug;

    unsigned int processed;
    unsigned int max_read_rate;
    unsigned int max_queue_size = (2 << 15);

    using OutputQueue = std::shared_ptr <BlockingConcurrentQueue<OutputType>>;

    SourceType source;
    OutputQueue output;

    SourceAdapter(std::string connection_string, unsigned int max_read_rate) :
            connection_string{connection_string}, max_read_rate{max_read_rate}, processed{0} {
        output = OutputQueue(new BlockingConcurrentQueue<OutputType>(max_queue_size));
    };

    void init();

    void pump();

    OutputQueue get_output_edge() {
        return output;
    }

    unsigned int queue_size() {
        // Source adapter reports the size of its output queue different from other adapters.
        return output->size_approx();
    }
};

// Specialize for specific input/output cases
template<>
void SourceAdapter<std::fstream, std::string>::pump() {
    //std::string data;
    //std::getline(source, data);

    static bool sent;
    while (not(sent = output->try_enqueue_bulk(&data_debug, 1))) {
        std::this_thread::sleep_for(std::chrono::nanoseconds(100));
    }
    processed += 1;
};

template<>
void SourceAdapter<std::fstream, std::string>::init() {
    source = std::fstream();
    source.open(connection_string);
    std::getline(source, data_debug);
};

typedef SourceAdapter<std::fstream, std::string> FileSourceAdapter;

// Sink Adapters

class SinkAdapterBase {
public:
    ~SinkAdapterBase() = default;

    virtual void init() = 0;
    virtual void pump() = 0;
};

template<class InputType>
class SinkAdapter : public SinkAdapterBase {
public:
    using InputQueue = std::shared_ptr <BlockingConcurrentQueue<InputType>>;

    std::string connection_string;
    unsigned int processed;
    InputQueue input;

    SinkAdapter() : connection_string(""), processed{0} {};

    SinkAdapter(std::string connection_string, InputQueue input) :
            connection_string{connection_string}, input{input}, processed{0} {};

    void init() override;

    void pump() override;

    unsigned int queue_size() {
        return input->size_approx();
    }
};

template<class InputType>
void SinkAdapter<InputType>::init() {};

template<class InputType>
void SinkAdapter<InputType>::pump() {};

template<class InputType>
class DiscardSinkAdapter : public SinkAdapter<InputType> {
public:
    using InputQueue = std::shared_ptr <BlockingConcurrentQueue<InputType>>;

    DiscardSinkAdapter() : SinkAdapter<InputType>() {};

    DiscardSinkAdapter(InputQueue input) : SinkAdapter<InputType>(input) {};

    void init() override {};

    void pump() override;
};

template<class InputType>
void DiscardSinkAdapter<InputType>::pump() {
    size_t count;
    InputType in[1];

    count = this->input->wait_dequeue_bulk_timed(&in[0], 1, std::chrono::milliseconds(10));
    if (count > 0) {
        this->processed += 1;
    }
}


// Stage Adapters
class StageAdapterBase {
public:
    ~StageAdapterBase() = default;

    virtual int pump() = 0;
};

template<class InputType, class OutputType>
class StageAdapter : public StageAdapterBase {
public:
    using InputQueue = std::shared_ptr <BlockingConcurrentQueue<InputType>>;
    using OutputQueue = std::shared_ptr <BlockingConcurrentQueue<OutputType>>;

    unsigned int processed = 0;
    unsigned int input_batch_size = 1;
    unsigned int output_batch_size = 1;
    unsigned int max_queue_size = 6 * (2 << 15);

    std::function<OutputType(InputType)> map;

    InputQueue input;
    OutputQueue output;

    std::unique_ptr<InputType> input_buffer = std::unique_ptr<InputType>(new InputType[1]);
    std::unique_ptr<OutputType> output_buffer = std::unique_ptr<OutputType>(new OutputType[1]);

    StageAdapter() {
        output = OutputQueue(new BlockingConcurrentQueue<OutputType>(max_queue_size));
    };

    StageAdapter(InputQueue input) : input{input} {
        output = OutputQueue(new BlockingConcurrentQueue<OutputType>(max_queue_size));
    };

    StageAdapter(std::function<OutputType(InputType)> map) :
            map{map} {
        output = OutputQueue(new BlockingConcurrentQueue<OutputType>(max_queue_size));
    };

    StageAdapter(std::function<OutputType(InputType)> map, InputQueue input) :
            map{map}, input{input} {
        output = OutputQueue(new BlockingConcurrentQueue<OutputType>(max_queue_size));
    };


    int pump() override;

    unsigned int queue_size() {
        return input->size_approx();
    }

    OutputQueue get_output_edge() {
        return output;
    }
};

template<class InputType, class OutputType>
int StageAdapter<InputType, OutputType>::pump() {
    bool sent;
    size_t count = 0;

    count = input->wait_dequeue_bulk_timed(input_buffer.get(), 1, std::chrono::milliseconds(10));
    if (count > 0) {
        *output_buffer.get() = map(*input_buffer.get());
        while (not(sent = output->try_enqueue_bulk(output_buffer.get(), 1))) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
            // do something while spinning?
        };

        processed += 1;
    }

    return processed;
};


template<class DataType>
class FilterAdapter : public StageAdapter<DataType, DataType> {
public:
    using InputQueue = std::shared_ptr <BlockingConcurrentQueue<DataType>>;

    std::function<bool(DataType)> filter;

    FilterAdapter(std::function<bool(DataType)> filter) :
            filter{filter}, StageAdapter<DataType, DataType>() {};

    FilterAdapter(std::function<bool(DataType)> filter, InputQueue input) :
            filter{filter}, StageAdapter<DataType, DataType>(input) {};

    int pump() override;
};

template<class DataType>
int FilterAdapter<DataType>::pump() {
    bool sent;
    size_t count = 0;
    DataType in[1];

    count = this->input->wait_dequeue_bulk_timed(&in[0], 1, std::chrono::milliseconds(10));
    if (count > 0) {
        if (this->filter(in[0])) {
            while (not(sent = this->output->try_enqueue_bulk(&in[0], 1))) {
                std::this_thread::sleep_for(std::chrono::nanoseconds(100));
                // do something while spinning?
            };
            this->processed += 1;
        }
    }

    return 1;
};


template<class DataType>
class RandomDropFilter : public FilterAdapter<DataType> {
public:
    static bool random_drop(DataType d) {
        return (std::rand() % 2 == 0);
    };

    RandomDropFilter() : FilterAdapter<DataType>(std::function<bool(DataType)>(random_drop)) {};
};


template<class InputType, class OutputType>
class RandomTrigWorkAdapter : public StageAdapter<InputType, OutputType> {
public:
    static constexpr double MYPI = 3.14159265;

    RandomTrigWorkAdapter() : StageAdapter<InputType, OutputType>(map) {};

    static RandomTrigWorkAdapter *create() {
        return new RandomTrigWorkAdapter<InputType, OutputType>();
    }

    static double trig_work() {
        int how_many = std::rand() % 1000000;
        double random_angle_rads = MYPI * ((double) std::rand() / (double) RAND_MAX);

        for (int i = 0; i < how_many;) {
            random_angle_rads = tan(atan(random_angle_rads));

            // Don't let GCC optimize us away
            __asm__ __volatile__("inc %[Incr]" : [Incr] "+r"(i));
        }

        return random_angle_rads;
    }

    static OutputType map(InputType data) {
        trig_work();
        return data;
    }
};


template<class InputType, class OutputType>
class ReplicationSubDivideWorkAdapter : public StageAdapter<InputType, OutputType> {
public:
    unsigned int buffer_sz;
    std::shared_ptr<OutputType[]> buffer_out;

    static ReplicationSubDivideWorkAdapter *create(unsigned int replica_count = 10) {
        return new ReplicationSubDivideWorkAdapter<InputType, OutputType>(replica_count);
    }

    ReplicationSubDivideWorkAdapter(unsigned int replica_count = 10) :
        buffer_sz{replica_count}, StageAdapter<InputType, OutputType>() {
        buffer_out = std::shared_ptr<OutputType[]>(new OutputType[replica_count]);
    };

    void map(InputType data) {
        for (int i = 0; i < buffer_sz; i++) {
            buffer_out[i] = data;
        }
    }

    int pump() override;
};

template<class InputType, class OutputType>
int ReplicationSubDivideWorkAdapter<InputType, OutputType>::pump() {
    bool sent;
    size_t count = 0;
    InputType in[1];

    count = this->input->wait_dequeue_bulk_timed(&in[0], 1, std::chrono::milliseconds(10));
    if (count > 0) {
        map(in[0]);
        while (not(sent = this->output->try_enqueue_bulk(buffer_out.get(), buffer_sz))) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
        }

        this->processed += 1;
    }

    return this->processed;
};


template<class DataType>
class BatchingWorkAdapter : public StageAdapter<DataType, std::shared_ptr<BatchObject<DataType>>> {
public:
    unsigned int timeout;
    unsigned int batch_size;

    static BatchingWorkAdapter *create(unsigned int batch_size = 10, unsigned int timeout = 10) {
        return new BatchingWorkAdapter<DataType>(batch_size, timeout);
    }

    BatchingWorkAdapter(unsigned int batch_size = 10, unsigned int timeout = 10) :
            batch_size{batch_size}, timeout{timeout},
            StageAdapter<DataType, std::shared_ptr<BatchObject<DataType>>>() {
    };

    int pump() override;
};

template<class DataType>
int BatchingWorkAdapter<DataType>::pump() {
    bool sent;
    size_t count = 0;

    //auto batch_object = new BatchObject<DataType>(batch_size);
    auto batch_object = std::shared_ptr<BatchObject<DataType>>(new BatchObject<DataType>(batch_size));

    count = this->input->wait_dequeue_bulk_timed(batch_object->batch.get(),
                                                 batch_size, std::chrono::milliseconds(timeout));
    if (count > 0) {
        // Possibly wasting memory here
        batch_object->batch_size = count;

        while (not(sent = this->output->try_enqueue_bulk(&batch_object, 1))) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
        }

        this->processed += count;
    }

    return this->processed;
};

// Example worker task
json map_string_to_json(std::string s) {
    return json::parse(s);
}

json map_random_work_on_json_object(json j) {
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

bool filter_random_drop(std::string d) {
    return (std::rand() % 2 == 0);
};


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
    enum StageType {
        source, intermediate, sink, conditional
    };
public:
    unsigned int stages = 0;
    bool print_stats = false;

    std::atomic<unsigned int> pipeline_running = 0;
    std::vector <std::atomic <unsigned int>*> stage_running_flags;
    std::vector <std::shared_ptr<void>> stage_adapters;
    std::vector <std::shared_ptr<void>> edges;
    std::vector <std::unique_ptr<TaskStats>> task_stats;
    std::map<unsigned int, tf::Task> id_to_task_map;
    std::vector <tf::Task> task_chain;

    tf::Task init, end;
    tf::Taskflow pipeline;
    tf::Executor &service_executor;

    ~LinearPipeline() {};

    LinearPipeline(tf::Executor &executor) : service_executor{executor} {
        init = pipeline.emplace([this]() {
            std::cout << "Initializing Pipeline" << std::endl;
        });

        end = pipeline.emplace([]() {
            std::cout << std::endl << std::flush << "\nTerminating Pipeline" << std::endl;
        });
    }

    LinearPipeline(tf::Executor &executor, bool print_stats) :
            service_executor{executor}, print_stats{print_stats} {
        init = pipeline.emplace([this]() {
            std::cout << "Initializing Pipeline" << std::endl;

            if (this->print_stats) {
                service_executor.async([this]() {
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
                                case StageType::conditional: {
                                    sstream << "[cond], ";
                                    break;
                                }
                                case StageType::source: {
                                    sstream << "[pumped]" << " => "
                                            << std::setw(10) << std::setprecision(0) << std::fixed <<
                                            this->task_stats[i]->avg_throughput << "(" <<
                                            this->task_stats[i]->avg_queue_size << "), ";
                                    break;
                                }

                                default: {
                                    sstream << "[q." << i << "] => "
                                            << std::setw(8) << std::setprecision(0) << std::fixed <<
                                            this->task_stats[i]->avg_throughput << "(" <<
                                            this->task_stats[i]->avg_queue_size << "), ";
                                }
                            }

                        }
                        sstream << " runtime: " << std::setw(8) << elapsed.count() << " sec\r";
                        std::cout << sstream.str() << std::flush;
                    }
                });
            }
        });
        end = pipeline.emplace([]() {
            std::cout << std::endl << std::flush << "\nTerminating Pipeline" << std::endl;
        });
    }

    LinearPipeline& start(unsigned int runtime = 0) {
        pipeline_running = 1;

        if (runtime > 0) {
            service_executor.async([this, runtime]() {
                std::this_thread::sleep_for(std::chrono::seconds(runtime));
                this->stop();
            });
        }

        service_executor.run(pipeline).wait();

        return *this;
    }

    LinearPipeline& stop() {
        pipeline_running = 0;

        return *this;
    }

    LinearPipeline& visualize(std::string filename) {
        std::ofstream file;
        file.open(filename);
        pipeline.dump(file);
        file.close();

        return *this;
    }

    template<class InputType, class OutputType>
    LinearLinkInfo<InputType, OutputType> add_stage(StageAdapter<InputType, OutputType> *adapter);

    template<class InputType, class OutputType>
    LinearLinkInfo<InputType, OutputType> add_stage(std::function<OutputType(InputType)> map);

    template<class InputType, class OutputType>
    LinearLinkInfo<InputType, OutputType> add_stage(OutputType(*map)(InputType)) {
        return add_stage(std::function(map));
    };

    // Redundant, but for experimenting with terminology.
    template<class InputType, class OutputType>
    LinearLinkInfo<InputType, OutputType> map(OutputType(*map)(InputType)) {
        return add_stage(std::function(map));
    };

    // Redundant, but for experimenting with terminology.
    template<class DataType>
    LinearLinkInfo<DataType, DataType>& filter(bool(*filter)(DataType)) {
        return add_stage(new FilterAdapter<DataType>(std::function<bool(DataType)>(filter)));
    };

    LinearPipeline& add_conditional_stage(unsigned int (*cond_test)(LinearPipeline *));

    template<class SourceType, class OutputType>
    LinearLinkInfo<SourceType, OutputType> set_source(std::string connection_string, unsigned int max_read_rate);

    template<class InputType>
    LinearLinkInfo<InputType, InputType> set_sink(SinkAdapter<InputType> *adapter);

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

LinearPipeline& LinearPipeline::add_conditional_stage(unsigned int (*cond_test)(LinearPipeline *)) {
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
    conditional.precede(task_chain[0], this->end);

    return *this;
}

template<class InputType, class OutputType>
LinearLinkInfo<InputType, OutputType> LinearPipeline::add_stage(StageAdapter<InputType, OutputType> *stage_adapter) {
    std::atomic<unsigned int> *run_flag = new std::atomic<unsigned int>(0);
    auto index = stage_running_flags.size();
    stage_running_flags.push_back(run_flag);

    std::cout << "Adding stage (adapter constructor) " << index << std::endl;
    std::shared_ptr <BlockingConcurrentQueue<InputType>> input =
            std::static_pointer_cast < BlockingConcurrentQueue < InputType >> (edges[index - 1]);

    auto adapter = std::shared_ptr<StageAdapter<InputType, OutputType>>(stage_adapter);
    adapter->input = input;

    stage_adapters.push_back(adapter);
    edges.push_back(adapter->get_output_edge());
    task_stats.push_back(std::unique_ptr<TaskStats>(new TaskStats()));
    task_stats[index]->last_visited = std::chrono::steady_clock::now();
    task_stats[index]->stage_type = StageType::intermediate;

    tf::Task stage_task = pipeline.emplace([this, index, adapter]() {
        unsigned int a = 0, b = 1;

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

    return LinearLinkInfo<InputType, OutputType>(*this);
}

template<class InputType, class OutputType>
LinearLinkInfo<InputType, OutputType> LinearPipeline::add_stage(std::function<OutputType(InputType)> map) {
    std::atomic<unsigned int> *run_flag = new std::atomic<unsigned int>(0);
    auto index = stage_running_flags.size();
    stage_running_flags.push_back(run_flag);

    std::cout << "Adding stage " << index << std::endl;
    std::shared_ptr <BlockingConcurrentQueue<InputType>> input =
            std::static_pointer_cast < BlockingConcurrentQueue < InputType >> (edges[index - 1]);

    auto adapter = std::shared_ptr<StageAdapter<InputType, OutputType>>(
            new StageAdapter<InputType, OutputType>(map, input));

    stage_adapters.push_back(adapter);
    edges.push_back(adapter->get_output_edge());
    task_stats.push_back(std::unique_ptr<TaskStats>(new TaskStats()));
    task_stats[index]->last_visited = std::chrono::steady_clock::now();
    task_stats[index]->stage_type = StageType::intermediate;

    tf::Task stage_task = pipeline.emplace([this, index, adapter]() {
        unsigned int a = 0, b = 1;

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

    return LinearLinkInfo<InputType, OutputType>(*this);
}

template<class SourceType, class OutputType>
LinearLinkInfo<SourceType, OutputType> LinearPipeline::set_source(std::string connection_string, unsigned int max_read_rate) {
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
                    adapter->pump();
                }
                this->stage_running_flags[index]->compare_exchange_strong(b, a);
            });
        }

        update_task_stats(index, adapter->processed, adapter->queue_size());
    }).name("source");

    id_to_task_map[index] = source_task;
    task_chain.push_back(source_task);
    init.precede(source_task);

    return LinearLinkInfo<SourceType, OutputType>(*this);
};

template<class InputType>
LinearLinkInfo<InputType, InputType> LinearPipeline::set_sink(SinkAdapter<InputType> *sink_adapter) {
    std::atomic<unsigned int> *run_flag = new std::atomic<unsigned int>(0);
    auto index = stage_running_flags.size();
    stage_running_flags.push_back(run_flag);

    std::shared_ptr <BlockingConcurrentQueue<InputType>> input =
            std::static_pointer_cast < BlockingConcurrentQueue < InputType >> (edges[index - 1]);

    sink_adapter->input = input;
    auto adapter = std::shared_ptr<SinkAdapter<InputType>>(sink_adapter);

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
    }).name("sink");

    id_to_task_map[index] = sink_task;
    task_chain.push_back(sink_task);
    task_chain[index - 1].precede(sink_task);

    return LinearLinkInfo<InputType, InputType>(*this);
};

template <class InputType, class OutputType>
class LinearLinkInfo {
public:
    LinearPipeline &lp;
    //std::shared_ptr <BlockingConcurrentQueue<OutputType>> link_output;

    LinearLinkInfo(LinearPipeline &lp) :
        lp{lp} {
    };

    LinearLinkInfo<OutputType, OutputType> set_sink(SinkAdapter<OutputType> *adapter) {
        return lp.set_sink(adapter);
    }

    template<class NextType>
    LinearLinkInfo<OutputType, NextType> add_stage(StageAdapter<OutputType, NextType> *adapter){
        return lp.add_stage(adapter);
    };

    template<class NextType>
    LinearLinkInfo<OutputType, NextType> add_stage(std::function<NextType(OutputType)> map) {
        return lp.add_stage(map);
    };

    template<class NextType>
    LinearLinkInfo<OutputType, NextType> add_stage(OutputType(*map)(InputType)) {
        return lp.add_stage(std::function(map));
    };

    template<class NextType>
    LinearLinkInfo<OutputType, NextType> map(NextType(*map)(OutputType)) {
        return lp.add_stage(std::function(map));
    };

    LinearLinkInfo<OutputType, OutputType> filter(bool(*filter)(OutputType)) {
        return lp.add_stage(new FilterAdapter<OutputType>(std::function<bool(OutputType)>(filter)));
    };

    LinearPipeline& add_conditional_stage(unsigned int (*cond_test)(LinearPipeline *)) {
        return lp.add_conditional_stage(cond_test);
    };
};

unsigned int map_conditional_jump_to_start(LinearPipeline *lp) {
    if (lp->pipeline_running) {
        return 0;
    }

    return 1;
}

#endif //TASKFLOW_BUILDING_BLOCKS_HPP
