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
class Batch {
public:
    // Maybe other info
    unsigned int batch_size;
    std::shared_ptr<DataType[]> batch;

    Batch() : batch_size{0}, batch{nullptr} {};

    Batch(unsigned int batch_size) : batch_size{batch_size} {
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

    using QueueType = BlockingConcurrentQueue<std::shared_ptr<OutputType>>;
    using OutputQueue = std::shared_ptr<QueueType>;

    SourceType source;
    OutputQueue output;

    SourceAdapter(std::string connection_string, unsigned int max_read_rate) :
            connection_string{connection_string}, max_read_rate{max_read_rate}, processed{0} {
        output = OutputQueue(new QueueType(max_queue_size));
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

// Specialize for specific input/output cases currently hacked to produce forever.
template<>
void SourceAdapter<std::fstream, std::string>::pump() {
    //std::string data;
    //std::getline(source, data);

    bool sent;
    std::shared_ptr<std::string> s(new std::string(data_debug));
    while (not(sent = output->try_enqueue_bulk(&s, 1))) {
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
    using InputQueue = std::shared_ptr <BlockingConcurrentQueue<std::shared_ptr<InputType>>>;

    std::string connection_string;
    unsigned int processed;
    InputQueue input;

    std::function<void(InputType *)> sink;

    SinkAdapter(std::function<void(InputType*)> sink) :
        connection_string(""), sink{sink}, processed{0} {};

    SinkAdapter(std::string connection_string, std::function<void(InputType*)> sink, InputQueue input) :
            connection_string{connection_string}, sink{sink}, input{input}, processed{0} {};

    void init() override;

    void pump() override;

    unsigned int queue_size() {
        return input->size_approx();
    }
};

template<class InputType>
void SinkAdapter<InputType>::init() {};

template<class InputType>
void SinkAdapter<InputType>::pump() {
    size_t count;
    std::shared_ptr<InputType> in;

    count = this->input->wait_dequeue_bulk_timed(&in, 1, std::chrono::milliseconds(10));
    if (count > 0) {
        sink(in.get());
        this->processed += 1;
    }
};

// Stage Adapters
class StageAdapterBase {
public:
    ~StageAdapterBase() = default;

    virtual int pump() = 0;
};

template<class InputType, class OutputType>
class StageAdapter : public StageAdapterBase {
public:
    using InputQueueType = BlockingConcurrentQueue<std::shared_ptr<InputType>>;
    using InputQueue = std::shared_ptr <InputQueueType>;

    using OutputQueueType = BlockingConcurrentQueue<std::shared_ptr<OutputType>>;
    using OutputQueue = std::shared_ptr <OutputQueueType>;

    unsigned int processed = 0;
    unsigned int input_batch_size = 1;
    unsigned int output_batch_size = 1;
    unsigned int max_queue_size = 6 * (2 << 15);

    std::function<OutputType*(InputType *)> map;

    InputQueue input;
    OutputQueue output;

    std::shared_ptr<InputType> input_buffer;
    std::shared_ptr<OutputType> output_buffer;

    StageAdapter() {
        output = OutputQueue(new OutputQueueType(max_queue_size));
    };

    StageAdapter(InputQueue input) : input{input} {
        output = OutputQueue(new OutputQueueType(max_queue_size));
    };

    StageAdapter(std::function<OutputType*(InputType *)> map) :
            map{map} {
        output = OutputQueue(new OutputQueueType(max_queue_size));
    };

    StageAdapter(std::function<OutputType*(InputType *)> map, InputQueue input) :
            map{map}, input{input} {
        output = OutputQueue(new OutputQueueType(max_queue_size));
    };

    int pump() override;

    unsigned int queue_size() {
        return input->size_approx();
    }

    OutputQueue get_output_edge() {
        return output;
    }
};

// Update so that we can take in output types for reference values, or shared_ptr's to types
// Then we only pass around objects that can clean themselves up
template<class InputType, class OutputType>
int StageAdapter<InputType, OutputType>::pump() {
    bool sent;
    size_t count = 0;

    count = input->wait_dequeue_bulk_timed(&input_buffer, 1, std::chrono::milliseconds(10));
    if (count > 0) {
        //std::stringstream sstream;
        //sstream << "Popped: " << *(in.get()) << std::endl;
        output_buffer = std::shared_ptr<OutputType>(map(input_buffer.get()));
        //std::cout << "Pushing: " << *output_buffer.get() << std::endl;
        while (not(sent = output->try_enqueue_bulk(&output_buffer, 1))) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
        };

        processed += 1;
    }

    return processed;
};

/*
template<class InputType, class OutputType>
class StageAdapter<Batch<InputType>, OutputType> : public StageAdapterBase {
public:
    using InputQueueType = BlockingConcurrentQueue<std::shared_ptr<Batch<InputType>>>;
    using InputQueue = std::shared_ptr <InputQueueType>;

    using OutputQueueType = BlockingConcurrentQueue<std::shared_ptr<OutputType>>;
    using OutputQueue = std::shared_ptr <OutputQueueType>;

    unsigned int processed = 0;
    unsigned int input_batch_size = 1;
    unsigned int output_batch_size = 1;
    unsigned int max_queue_size = 6 * (2 << 15);

    std::function<OutputType*(InputType *)> map;

    InputQueue input;
    OutputQueue output;

    std::shared_ptr<Batch<InputType>> input_buffer;
    std::shared_ptr<OutputType> output_buffer;

    StageAdapter() {
        output = OutputQueue(new OutputQueueType(max_queue_size));
    };

    StageAdapter(InputQueue input) : input{input} {
        output = OutputQueue(new OutputQueueType(max_queue_size));
    };

    StageAdapter(std::function<OutputType*(InputType *)> map) :
            map{map} {
        output = OutputQueue(new OutputQueueType(max_queue_size));
    };

    StageAdapter(std::function<OutputType*(InputType *)> map, InputQueue input) :
            map{map}, input{input} {
        output = OutputQueue(new OutputQueueType(max_queue_size));
    };

    int pump() override;

    unsigned int queue_size() {
        return input->size_approx();
    }

    OutputQueue get_output_edge() {
        return output;
    }
};

// Update so that we can take in output types for reference values, or shared_ptr's to types
// Then we only pass around objects that can clean themselves up
template<class InputType, class OutputType>
int StageAdapter<Batch<InputType>, OutputType>::pump() {
    bool sent;
    size_t count = 0;

    count = input->wait_dequeue_bulk_timed(&input_buffer, 1, std::chrono::milliseconds(10));
    if (count > 0) {
        //Iterate over the batch
        for (int i = 0; i < input_buffer->batch_size; i++) {
            output_buffer = std::shared_ptr<OutputType>(
                        map(input_buffer->batch[i].get())
                    );
            while (not(sent = output->try_enqueue_bulk(&output_buffer, 1))) {
                std::this_thread::sleep_for(std::chrono::nanoseconds(100));
            };
        }
        processed += 1;
    }

    return processed;
};
*/

template<class DataType>
class FilterAdapter : public StageAdapter<DataType, DataType> {
public:
    using InputQueueType = BlockingConcurrentQueue<std::shared_ptr<DataType>>;
    using InputQueue = std::shared_ptr<InputQueueType>;

    std::function<bool(DataType *)> filter;

    FilterAdapter(std::function<bool(DataType *)> filter) :
            filter{filter}, StageAdapter<DataType, DataType>() {};

    FilterAdapter(std::function<bool(DataType *)> filter, InputQueue input) :
            filter{filter}, StageAdapter<DataType, DataType>(input) {};

    int pump() override;
};

template<class DataType>
int FilterAdapter<DataType>::pump() {
    bool sent;
    size_t count = 0;
    std::shared_ptr<DataType> in;

    count = this->input->wait_dequeue_bulk_timed(&in, 1, std::chrono::milliseconds(10));
    if (count > 0) {
        if (this->filter(in.get())) {
            while (not(sent = this->output->try_enqueue_bulk(&in, 1))) {
                std::this_thread::sleep_for(std::chrono::nanoseconds(100));
            };
            this->processed += 1;
        }
    }

    return 1;
};

template<class InputType, class OutputType>
class ExplodeAdapter : public StageAdapter<InputType, OutputType> {
public:
    using InputQueueType = BlockingConcurrentQueue<std::shared_ptr<InputType>>;
    using InputQueue = std::shared_ptr <InputQueueType>;

    using FunctionSignature = std::function<std::tuple<OutputType**, unsigned int>(InputType*)>;

    FunctionSignature exploder;

    ExplodeAdapter(FunctionSignature exploder) :
        exploder{exploder}, StageAdapter<InputType, OutputType>() {};

    ExplodeAdapter(FunctionSignature exploder, InputQueue input) :
        exploder{exploder}, StageAdapter<InputType, OutputType>(input) {};

    int pump() override;
};

template<class InputType, class OutputType>
int ExplodeAdapter<InputType, OutputType>::pump() {
    bool sent;
    size_t count = 0;
    unsigned int buffer_sz;

    OutputType **out;
    std::shared_ptr<InputType> in;

    count = this->input->wait_dequeue_bulk_timed(&in, 1, std::chrono::milliseconds(10));
    if (count > 0) {
        std::tie(out, buffer_sz) = exploder(in.get());
        std::shared_ptr<OutputType> buffer_out[buffer_sz];
        for(int i = 0; i < buffer_sz; i++) {
            buffer_out[i] = std::shared_ptr<OutputType>(out[i]);
        }
        delete out;

        while (not(sent = this->output->try_enqueue_bulk(&buffer_out[0], buffer_sz))) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
        }

        this->processed += 1;
    }

    return this->processed;
};

template<class DataType>
class BatchingWorkAdapter : public StageAdapter<DataType, Batch<DataType>> {
public:
    unsigned int timeout;
    unsigned int batch_size;

    BatchingWorkAdapter(unsigned int batch_size = 10, unsigned int timeout = 10) :
            batch_size{batch_size}, timeout{timeout},
            StageAdapter<DataType, Batch<DataType>>() {
    };

    int pump() override;
};

template<class DataType>
int BatchingWorkAdapter<DataType>::pump() {
    bool sent;
    size_t count = 0;

    std::shared_ptr<DataType> thebatch[batch_size];

    count = this->input->wait_dequeue_bulk_timed(&thebatch[0],
                                                 batch_size, std::chrono::milliseconds(timeout));

    if (count > 0) {
        auto batch_object = std::shared_ptr<Batch<DataType>>(new Batch<DataType>(count));
        for(int i = 0; i < count; i++) {
            batch_object->batch[i] = DataType(*thebatch[i].get());
        }

        while (not(sent = this->output->try_enqueue_bulk(&batch_object, 1))) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
        }

        this->processed += count;
    }

    return this->processed;
};

// Example worker task
json* map_string_to_json(std::string *s) {
    auto j = new json(json::parse(*s));

    //std::cout << typeid(j).name() << std::endl;
    //std::cout << *j << std::endl;

    return j;
}

json* map_random_work_on_json_object(json *_j) {
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

template<typename DataType>
bool filter_random_drop(DataType *d) {
    return (std::rand() % 2 == 0);
};

template<typename DataType>
DataType* map_random_trig_work(DataType *d) {
    double MYPI = 3.14159265;
    int how_many = std::rand() % 10000;
    double random_angle_rads = MYPI * ((double) std::rand() / (double) RAND_MAX);

    for (int i = 0; i < how_many;) {
        random_angle_rads = tan(atan(random_angle_rads));

        // Don't let GCC optimize us away
        __asm__ __volatile__("inc %[Incr]" : [Incr] "+r"(i));
    }

    return new DataType(*d);
}

template<typename InputType, typename OutputType>
std::tuple<OutputType**, unsigned int> exploder_duplicate(OutputType *d) {
    int k = 10;
    OutputType** out = new OutputType*[k];

    for (int i = 0; i < k; i++) {
        out[i] = new OutputType(*d);
    }

    return std::tuple(out, k);
}

template<typename DataType>
std::tuple<DataType**, unsigned int> exploder_duplicate(DataType *d) {
    int k = 10;
    DataType** out = new DataType*[k];

    for (int i = 0; i < k; i++) {
        out[i] = new DataType(*d);
    }

    return std::tuple(out, k);
}

template<typename InputType>
void sink_discard(InputType *) {
    return;
}

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
    LinearLinkInfo<InputType, OutputType> add_stage(std::function<OutputType*(InputType*)> map);

    template<class InputType, class OutputType>
    LinearLinkInfo<InputType, OutputType> add_stage(OutputType*(*map)(InputType*)) {
        return add_stage(std::function(map));
    };

    // Redundant, but for experimenting with terminology.
    template<class InputType, class OutputType>
    LinearLinkInfo<InputType, OutputType> map(OutputType*(*map)(InputType*)) {
        return add_stage(std::function(map));
    };

    // Redundant, but for experimenting with terminology.
    template<class DataType>
    LinearLinkInfo<DataType, DataType> filter(bool(*filter)(DataType *)) {
        return add_stage(new FilterAdapter<DataType>(std::function<bool(DataType *)>(filter)));
    };

    template<class DataType>
    LinearLinkInfo<DataType, Batch<DataType>> batch(
            unsigned int batch_size, unsigned int timeout_ms) {
        return add_stage(new BatchingWorkAdapter<DataType>(batch_size, timeout_ms));
    };

    template<class InputType, class OutputType>
    LinearLinkInfo<InputType, OutputType> explode(
            std::tuple<OutputType**, unsigned int>(*exploder)(InputType*)) {
        return add_stage(new ExplodeAdapter<InputType, OutputType>(exploder));
    };

    LinearPipeline& add_conditional_stage(unsigned int (*cond_test)(LinearPipeline *));

    template<class SourceType, class OutputType>
    LinearLinkInfo<SourceType, OutputType> set_source(std::string connection_string,
                                                      unsigned int max_read_rate);

    template<class InputType>
    LinearLinkInfo<InputType, InputType> set_sink(SinkAdapter<InputType> *sink_adapter);

    template<class InputType>
    LinearLinkInfo<InputType, InputType> set_sink(std::string connection_string,
                                                  std::function<void(InputType*)> sink);

    template<class InputType>
    LinearLinkInfo<InputType, InputType> set_sink(std::string connection_string,
                                                  void(*sink)(InputType*)) {
        return set_sink(connection_string, std::function<void(InputType*)>(sink));
    };


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
    std::shared_ptr <BlockingConcurrentQueue<std::shared_ptr<InputType>>> input =
            std::static_pointer_cast<BlockingConcurrentQueue<std::shared_ptr<InputType>>> (edges[index - 1]);

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
LinearLinkInfo<InputType, OutputType> LinearPipeline::add_stage(std::function<OutputType*(InputType*)> map) {
    std::atomic<unsigned int> *run_flag = new std::atomic<unsigned int>(0);
    auto index = stage_running_flags.size();
    stage_running_flags.push_back(run_flag);

    std::cout << "Adding stage " << index << std::endl;
    std::shared_ptr <BlockingConcurrentQueue<std::shared_ptr<InputType>>> input =
            std::static_pointer_cast<BlockingConcurrentQueue<std::shared_ptr<InputType>>> (edges[index - 1]);

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
                    if (this->task_stats[index]->avg_throughput <= adapter->max_read_rate) {
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

    return LinearLinkInfo<SourceType, OutputType>(*this);
};

template<class InputType>
LinearLinkInfo<InputType, InputType> LinearPipeline::set_sink(SinkAdapter<InputType> *sink_adapter) {
    std::atomic<unsigned int> *run_flag = new std::atomic<unsigned int>(0);
    auto index = stage_running_flags.size();
    stage_running_flags.push_back(run_flag);

    std::shared_ptr <BlockingConcurrentQueue<std::shared_ptr<InputType>>> input =
            std::static_pointer_cast < BlockingConcurrentQueue<std::shared_ptr<InputType>>> (edges[index - 1]);

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

template<class InputType>
LinearLinkInfo<InputType, InputType> LinearPipeline::set_sink(std::string connection_string,
        std::function<void(InputType*)> sink) {
    std::atomic<unsigned int> *run_flag = new std::atomic<unsigned int>(0);
    auto index = stage_running_flags.size();
    stage_running_flags.push_back(run_flag);

    std::shared_ptr <BlockingConcurrentQueue<std::shared_ptr<InputType>>> input =
            std::static_pointer_cast < BlockingConcurrentQueue<std::shared_ptr<InputType>>> (edges[index - 1]);

    auto adapter = std::shared_ptr<SinkAdapter<InputType>>(
            new SinkAdapter<InputType>(connection_string, sink, input));

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


template <class SourceLinkInput, class LinkOutputType>
class LinearLinkInfo {
public:
    LinearPipeline &lp;

    LinearLinkInfo(LinearPipeline &lp) :
        lp{lp} {
    };

    LinearLinkInfo<LinkOutputType, LinkOutputType> set_sink(SinkAdapter<LinkOutputType> *adapter) {
        return lp.set_sink(adapter);
    }

    LinearLinkInfo<LinkOutputType, LinkOutputType> set_sink(std::string connection_string,
                                                    std::function<void(LinkOutputType*)> sink) {
        return lp.set_sink(connection_string, sink);
    }

    LinearLinkInfo<LinkOutputType, LinkOutputType> set_sink(std::string connection_string,
                                                    void(*sink)(LinkOutputType*)) {
        return lp.set_sink(connection_string, sink);
    }

    template<class NextType>
    LinearLinkInfo<LinkOutputType, NextType> add_stage(StageAdapter<LinkOutputType, NextType> *adapter){
        return lp.add_stage(adapter);
    };

    template<class NextType>
    LinearLinkInfo<LinkOutputType, NextType> add_stage(std::function<NextType*(LinkOutputType*)> map) {
        return lp.add_stage(map);
    };

    template<class NextType>
    LinearLinkInfo<LinkOutputType, NextType> add_stage(LinkOutputType*(*map)(LinkOutputType*)) {
        return lp.add_stage(std::function(map));
    };

    template<class NextType>
    LinearLinkInfo<LinkOutputType, NextType> map(NextType*(*map)(LinkOutputType*)) {
        return lp.add_stage(std::function(map));
    };

    LinearLinkInfo<LinkOutputType, LinkOutputType> filter(bool(*filter)(LinkOutputType*)) {
        return lp.add_stage(new FilterAdapter<LinkOutputType>(std::function<bool(LinkOutputType *)>(filter)));
    };

    LinearLinkInfo<LinkOutputType, Batch<LinkOutputType>> batch(unsigned int batch_size, unsigned int timeout_ms) {
        return lp.batch<LinkOutputType>(batch_size, timeout_ms);
    }

    template<class NextType>
    LinearLinkInfo<LinkOutputType, NextType> explode(
        std::tuple<NextType**, unsigned int>(*exploder)(LinkOutputType*)) {
        return lp.explode(exploder);
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
