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
name.
remove_prefix(prefix
.

size()

);
name.
remove_suffix(suffix
.

size()

);
return
name;
};


class PipelineStageConstructor;

class LinearPipeline;

template<class InputType, class OutputType>
class LinearLinkInfo;

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

class StageAdapterBase {
public:
    ~StageAdapterBase() = default;

    virtual void init() = 0;

    virtual void pump() = 0;

    virtual unsigned int queue_size() = 0;
};

template<class InputType, class OutputType>
class StageAdapter : public StageAdapterBase {
public:
    using InputQueueType = BlockingConcurrentQueue <std::shared_ptr<InputType>>;
    using InputQueue = std::shared_ptr<InputQueueType>;

    using OutputQueueType = BlockingConcurrentQueue <std::shared_ptr<OutputType>>;
    using OutputQueue = std::shared_ptr<OutputQueueType>;

    std::atomic<unsigned int> running = 0;

    unsigned int processed = 0;
    unsigned int input_buffer_size = 1;
    unsigned int output_buffer_size = 1;
    unsigned int max_queue_size = 6 * (2 << 15);

    InputQueue input;
    size_t read_count;
    std::shared_ptr <InputType> input_buffer;

    OutputQueue output;
    std::shared_ptr <OutputType> output_buffer;

    StageAdapter() {
        output = OutputQueue(new OutputQueueType(max_queue_size));
    };

    StageAdapter(bool create_queue) {
        if (create_queue) {
            output = OutputQueue(new OutputQueueType(max_queue_size));
        }
    };

    StageAdapter(InputQueue input) : input{input} {
        output = OutputQueue(new OutputQueueType(max_queue_size));
    };

    void init() override { return; }

    void pump() override { return; };

    unsigned int queue_size() override {
        return input->size_approx();
    }

    OutputQueue get_output_edge() {
        return output;
    }
};

class FileSourceAdapter : public StageAdapter<void, std::string> {
public:
    unsigned int max_read_rate;
    std::string connection_string;
    std::string data_debug; // TODO: only for testing
    std::fstream input_file_stream;

    FileSourceAdapter(std::string connection_string, unsigned int max_read_rate) :
            connection_string{connection_string}, max_read_rate{max_read_rate},
            StageAdapter<void, std::string>() {};

    void init() override;

    void pump() override;

    unsigned int queue_size() override {
        return this->output->size_approx();
    };
};

void FileSourceAdapter::init() {
    input_file_stream = std::fstream();
    input_file_stream.open(connection_string);

    //TODO: For debugging
    std::getline(input_file_stream, data_debug);
};

void FileSourceAdapter::pump() {
    //std::string data;
    //std::getline(source, data);

    this->output_buffer = std::shared_ptr<std::string>(new std::string(data_debug));
    while (this->running == 1 && not(this->output->try_enqueue_bulk(&this->output_buffer, 1))) {
        std::this_thread::sleep_for(std::chrono::nanoseconds(100));
    }

    this->processed += 1;
};

template<class InputType>
class DiscardSinkAdapter : public StageAdapter<InputType, void> {
public:
    using InputQueue = std::shared_ptr <BlockingConcurrentQueue<std::shared_ptr < InputType>>>;

    std::string connection_string;
    std::function<void(InputType *)> sink;

    DiscardSinkAdapter(std::string connection_string, std::function<void(InputType *)> sink) :
            connection_string(""), sink{sink},
            StageAdapter<InputType, void>(false) {};

    void init() override;

    void pump() override;

    unsigned int queue_size() {
        return this->input->size_approx();
    }
};

template<class InputType>
void DiscardSinkAdapter<InputType>::init() {
};

template<class InputType>
void DiscardSinkAdapter<InputType>::pump() {
    this->read_count = this->input->wait_dequeue_bulk_timed(&this->input_buffer,
                                                            this->input_buffer_size, std::chrono::milliseconds(10));
    if (this->read_count > 0) {
        sink(this->input_buffer.get());
        this->processed += 1;
    }
};

template<class InputType, class OutputType>
class MapAdapter : public StageAdapter<InputType, OutputType> {
public:
    using InputQueueType = BlockingConcurrentQueue <std::shared_ptr<InputType>>;
    using InputQueue = std::shared_ptr<InputQueueType>;

    using MapSignature = std::function<OutputType *(InputType *)>;

    MapSignature map;

    MapAdapter() : StageAdapter<InputType, OutputType>() {};

    MapAdapter(MapSignature map) :
            map{map}, StageAdapter<InputType, OutputType>() {};

    MapAdapter(MapSignature map, InputQueue input) :
            map{map}, StageAdapter<InputType, OutputType>(input) {};

    void pump() override;
};

template<class InputType, class OutputType>
void MapAdapter<InputType, OutputType>::pump() {
    this->read_count = this->input->wait_dequeue_bulk_timed(&this->input_buffer,
                                                            this->input_buffer_size, std::chrono::milliseconds(10));
    if (this->read_count > 0) {
        this->output_buffer = std::shared_ptr<OutputType>(map(this->input_buffer.get()));
        while (this->running == 1 &&
               not(this->output->try_enqueue_bulk(&this->output_buffer, this->output_buffer_size))) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
        };

        this->processed += 1;
    }
};

template<class DataType>
class FilterAdapter : public StageAdapter<DataType, DataType> {
public:
    using InputQueueType = BlockingConcurrentQueue <std::shared_ptr<DataType>>;
    using InputQueue = std::shared_ptr<InputQueueType>;

    std::function<bool(DataType *)> filter;

    FilterAdapter(std::function<bool(DataType *)> filter) :
            filter{filter}, StageAdapter<DataType, DataType>() {};

    FilterAdapter(std::function<bool(DataType *)> filter, InputQueue input) :
            filter{filter}, StageAdapter<DataType, DataType>(input) {};

    void pump() override;
};

template<class DataType>
void FilterAdapter<DataType>::pump() {
    this->read_count = this->input->wait_dequeue_bulk_timed(&this->input_buffer,
                                                            this->input_buffer_size, std::chrono::milliseconds(10));
    if (this->read_count > 0) {
        if (this->filter(this->input_buffer.get())) {
            while (this->running == 1 &&
                   not(this->output->try_enqueue_bulk(&this->input_buffer, this->output_buffer_size))) {
                std::this_thread::sleep_for(std::chrono::nanoseconds(100));
            };
            this->processed += 1;
        }
    }
};

template<class InputType, class OutputType>
class ExplodeAdapter : public StageAdapter<InputType, OutputType> {
public:
    using InputQueueType = BlockingConcurrentQueue <std::shared_ptr<InputType>>;
    using InputQueue = std::shared_ptr<InputQueueType>;

    using FunctionSignature = std::function<std::tuple<OutputType **, unsigned int>(InputType * )>;

    FunctionSignature exploder;

    ExplodeAdapter(FunctionSignature exploder) :
            exploder{exploder}, StageAdapter<InputType, OutputType>() {};

    ExplodeAdapter(FunctionSignature exploder, InputQueue input) :
            exploder{exploder}, StageAdapter<InputType, OutputType>(input) {};

    void pump() override;
};

template<class InputType, class OutputType>
void ExplodeAdapter<InputType, OutputType>::pump() {
    unsigned int buffer_sz;
    OutputType **out;

    //TODO: use base class buffer vars better.

    this->read_count = this->input->wait_dequeue_bulk_timed(&this->input_buffer, this->input_buffer_size,
                                                            std::chrono::milliseconds(10));
    if (this->read_count > 0) {
        std::tie(out, buffer_sz) = exploder(this->input_buffer.get());
        std::shared_ptr <OutputType> buffer_out[buffer_sz];
        for (int i = 0; i < buffer_sz; i++) {
            buffer_out[i] = std::shared_ptr<OutputType>(out[i]);
        }
        delete out;

        while (this->running == 1 && not(this->output->try_enqueue_bulk(&buffer_out[0], buffer_sz))) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
        }

        this->processed += 1;
    }
};

template<class DataType>
class BatchAdapter : public StageAdapter<DataType, Batch<DataType>> {
public:
    unsigned int timeout;
    unsigned int batch_size;

    BatchAdapter(unsigned int batch_size = 10, unsigned int timeout = 10) :
            batch_size{batch_size}, timeout{timeout},
            StageAdapter<DataType, Batch<DataType>>() {
    };

    void pump() override;
};

template<class DataType>
void BatchAdapter<DataType>::pump() {
    std::shared_ptr <DataType> thebatch[batch_size];

    this->read_count = this->input->wait_dequeue_bulk_timed(&thebatch[0],
                                                            batch_size, std::chrono::milliseconds(timeout));
    if (this->read_count > 0) {
        auto batch_object = std::shared_ptr<Batch<DataType>>(new Batch<DataType>(this->read_count));
        for (int i = 0; i < this->read_count; i++) {
            batch_object->batch[i] = DataType(*thebatch[i].get());
        }

        while (this->running == 1 && not(this->output->try_enqueue_bulk(&batch_object, 1))) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(100));
        }

        this->processed += this->read_count;
    }
};

// Example worker task
json *map_string_to_json(std::string *s) {
    return new json(json::parse(*s));
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

template<typename DataType>
bool filter_random_drop(DataType *d) {
    return (std::rand() % 2 == 0);
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

template<>
json *map_random_trig_work(json *d) {
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

template<typename InputType, typename OutputType>
std::tuple<OutputType **, unsigned int> exploder_duplicate(OutputType *d) {
    int k = 10;
    OutputType **out = new OutputType *[k];

    for (int i = 0; i < k; i++) {
        out[i] = new OutputType(*d);
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
    std::map <std::string, StageType> typename_to_stagetype;

    unsigned int stages = 0;
    bool print_stats = false;

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

    PipelineStageConstructor *psc;
    tf::Task init, start_task, end;
    tf::Taskflow pipeline;
    tf::Executor &service_executor;

    ~LinearPipeline() {};

    LinearPipeline(tf::Executor &executor, PipelineStageConstructor *psc) :
            psc{psc}, service_executor{executor} {

        init = pipeline.emplace([]() {
            //std::cout << "Initializing Pipeline" << std::endl;
        });

        end = pipeline.emplace([]() {
            //std::cout << std::endl << std::flush << "\nTerminating Pipeline" << std::endl;
        });
    }

    LinearPipeline(tf::Executor &executor, PipelineStageConstructor *psc, bool print_stats) :
            psc{psc}, service_executor{executor}, print_stats{print_stats} {

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

    LinearPipeline &add_stage_by_name(std::string adapter_type,
                                      std::string input_type,
                                      std::string output_type) {


        return *this;
    }

    template<class InputType, class OutputType>
    LinearLinkInfo<InputType, OutputType> add_stage(StageAdapter<InputType, OutputType> *adapter, StageType type);

    template<class OutputType>
    LinearLinkInfo<void, OutputType> source(StageAdapter<void, OutputType> *adapter) {
        //TODO: init framework
        return add_stage(adapter, StageType::op_source);
    }

    template<class InputType>
    LinearLinkInfo<InputType, void> sink(void(*sink)(InputType *)) {
        //TODO: init framework
        return add_stage(new DiscardSinkAdapter<InputType>(std::string(""), sink), StageType::op_sink);
    }

    template<class InputType, class OutputType>
    LinearLinkInfo<InputType, OutputType> map(OutputType *(*map)(InputType *)) {
        return add_stage(new MapAdapter<InputType, OutputType>(std::function<OutputType *(InputType *)>(map)),
                         StageType::op_map);
    };

    template<class DataType>
    LinearLinkInfo<DataType, DataType> filter(bool(*filter)(DataType *)) {
        return add_stage(new FilterAdapter<DataType>(std::function<bool(DataType *)>(filter)),
                         StageType::op_filter);
    };

    template<class DataType>
    LinearLinkInfo<DataType, Batch<DataType>> batch(
            unsigned int batch_size, unsigned int timeout_ms) {
        return add_stage(new BatchAdapter<DataType>(batch_size, timeout_ms),
                         StageType::op_batch);
    };

    template<class InputType, class OutputType>
    LinearLinkInfo<InputType, OutputType> explode(
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
    //std::cout << "Adding conditional at " << index << std::endl;

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
LinearLinkInfo<InputType, OutputType> LinearPipeline::add_stage(StageAdapter<InputType,
        OutputType> *stage_adapter,
                                                                StageType type) {
    //std::cout << "Adding stage (adapter constructor) " << index << std::endl;
    auto index = stages++;
    auto adapter = std::shared_ptr<StageAdapter<InputType, OutputType>>(stage_adapter);

    json stage = json::object({
                                      {"index",       index},
                                      {"input_type",  type_name<InputType>()},
                                      {"output_type", type_name<OutputType>()}
                              });
    blueprint["pipeline"]["stages"].push_back(stage);
    std::cout << stage.dump(4) << std::endl;

    if (type != StageType::op_source) {
        std::shared_ptr < BlockingConcurrentQueue < std::shared_ptr < InputType>>> input =
                                                                                           std::static_pointer_cast <
                                                                                           BlockingConcurrentQueue <
                                                                                           std::shared_ptr <
                                                                                           InputType>>> (edges[index -
                                                                                                               1]);

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

            service_executor.silent_async([&]() {
                while (this->pipeline_running == 1) {
                    adapter->pump();
                }
                adapter->running = 0;
            });
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

    return LinearLinkInfo<InputType, OutputType>(*this);
}

template<class SourceLinkInput, class LinkOutputType>
class LinearLinkInfo {
public:
    LinearPipeline &lp;

    LinearLinkInfo(LinearPipeline &lp) :
            lp{lp} {
    };

    template<class NextType>
    LinearLinkInfo<LinkOutputType, NextType> add_stage(StageAdapter<LinkOutputType, NextType> *adapter) {
        return lp.add_stage<LinkOutputType, NextType>(adapter);
    };

    LinearLinkInfo<void, LinkOutputType> source(StageAdapter<void, LinkOutputType> *adapter) {
        return lp.source<void, LinkOutputType>(adapter);
    };

    LinearLinkInfo<LinkOutputType, void> sink(void(*sink)(LinkOutputType *)) {
        return lp.sink<LinkOutputType>(sink);
    };

    template<class NextType>
    LinearLinkInfo<LinkOutputType, NextType> map(NextType *(*map)(LinkOutputType *)) {
        return lp.map<LinkOutputType, NextType>(map);
    };

    LinearLinkInfo<LinkOutputType, LinkOutputType> filter(bool(*filter)(LinkOutputType *)) {
        return lp.filter<LinkOutputType>(filter);
    };

    LinearLinkInfo<LinkOutputType, Batch<LinkOutputType>> batch(unsigned int batch_size, unsigned int timeout_ms) {
        return lp.batch<LinkOutputType>(batch_size, timeout_ms);
    }

    template<class NextType>
    LinearLinkInfo<LinkOutputType, NextType> explode(
            std::tuple<NextType **, unsigned int>(*exploder)(LinkOutputType *)) {
        return lp.explode<LinkOutputType, NextType>(exploder);
    };

    LinearPipeline &add_conditional_stage(unsigned int (*cond_test)(LinearPipeline *)) {
        return lp.add_conditional_stage(cond_test);
    };
};


template<typename T, typename U>
struct TestAdapter {
};


template<const char *name>
struct name_to_type {
};


static constexpr char string_name[] = "string";
template<>
struct name_to_type<string_name> {
    typedef std::string type;
};

static constexpr char json_name[] = "json";
template<>
struct name_to_type<json_name> {
    typedef json type;
};


class PipelineStageConstructor {
    static PipelineStageConstructor *factory;

    PipelineStageConstructor() {}
public:
    std::map <std::string, std::string> supported_types_map;
    std::map <std::string, std::string> map_to_function_links;

    static PipelineStageConstructor *get() {
        if (!factory) {
            factory = new PipelineStageConstructor;
        }
        return PipelineStageConstructor::factory;
    }

    template<const char *T_name, const char *U_name>
    decltype(auto) create_map_adapter() {
        using input_type = typename name_to_type<T_name>::type;
        using output_type = typename name_to_type<U_name>::type;

        auto adapter = std::shared_ptr<StageAdapter<input_type, output_type>>(
                new MapAdapter<input_type, output_type>());

        return adapter;
    }

    template<typename T1, typename T2>
    bool instantiate_adapters() {
        create_map_adapter<T1, T2>();
        create_map_adapter<T2, T2>();

        return true;
    }

    // We can return dynamically constructed elements from here.
    template<typename InputType>
    void splice_output(std::string out_id, std::string op_name) {
        if (type_name<std::string>() == out_id) {
            auto adapter = std::shared_ptr<StageAdapter<InputType, std::string>>(
                    new MapAdapter<InputType, std::string>());
            std::cout << "Created a thing: " << type_name<decltype(adapter)>() << std::endl;
        } else if (type_name<json>() == out_id) {
            std::cout << "Out is a json object" << std::endl;
            auto adapter = std::shared_ptr<StageAdapter<InputType, json>>(
                    new MapAdapter<InputType, json>());
            std::cout << "Created a thing: " << type_name<decltype(adapter)>() << std::endl;
        } else {
            std::stringstream sstream;
            sstream << "Unsupported Output Type: " << supported_types_map[out_id] << std::endl;
            throw (sstream.str());
        }
    }

    void create(std::string type, std::string in, std::string out, std::string op_name) {
        auto in_id = supported_types_map[in];
        auto out_id = supported_types_map[out];

        std::cout << "Creating: " << in_id << "*(*map)(" << out_id << "*)" << std::endl;

        if (type_name<std::string>() == in_id) {
            std::cout << "In is a string" << std::endl;
            splice_output<std::string>(out_id, op_name);
        } else if (type_name<json>() == in_id) {
            std::cout << "In is a json object" << std::endl;
            splice_output<json>(out_id, op_name);
        } else {
            std::stringstream sstream;
            sstream << "Unsupported Input Type: " << supported_types_map[in] << std::endl;
            throw (sstream.str());
        }
    }

    /*
    template<class Tin1, class Tin2>
    std::function<StageAdapter<Tin1, Tin2> *(std::function<Tin2*(Tin1*)>)>
    construct_map() {
        auto func = [](std::function<Tin2*(Tin1*)> map) {
            return new MapAdapter<Tin1, Tin2>(map);
        };

        return func;
    };
    */

    template<class Tin1, class Tin2>
    void do_init(std::string Id1, std::string Id2) {
        supported_types_map[Id1] = type_name<Tin1>();
        supported_types_map[Id2] = type_name<Tin2>();
    };

    // Want to take in a list of T Args, and instantiate constructors for all pairs of types
    template<class Tin1, class Tin2>
    void init_stage_constructors(std::string Id1, std::string Id2) {
        do_init<Tin1, Tin2>(Id1, Id2);
        do_init<Tin2, Tin1>(Id2, Id1);
    };
};

PipelineStageConstructor *PipelineStageConstructor::factory = nullptr;

template <class... Ts> struct Constructor {
    template<const char *T_name, const char *U_name, typename... Args>
    decltype(auto) get_map_adapter(Args&&... args) {
        using input_type = typename name_to_type<T_name>::type;
        using output_type = typename name_to_type<U_name>::type;

        return create_map_adapter<input_type, output_type>(std::forward<Args>(args)...);
    }
};

template <class T1, class T2, class...Ts>
struct Constructor<T1, T2, Ts...> : Constructor<T1, Ts...>, Constructor<T2, Ts...> {
    std::shared_ptr<StageAdapter<T1, T1>> create_map_adapter(T1*(*map)(T1*)) {
        auto adapter = std::shared_ptr<StageAdapter<T1, T1>>(
                new MapAdapter<T1, T1>());

        return adapter;
    };

    std::shared_ptr<StageAdapter<T2, T2>> create_map_adapter(T2*(*map)(T2*)) {
        auto adapter = std::shared_ptr<StageAdapter<T2, T2>>(
                new MapAdapter<T2, T2>());

        return adapter;
    };

    std::shared_ptr<StageAdapter<T1, T2>> create_map_adapter(T1*(*map)(T2*)) {
        auto adapter = std::shared_ptr<StageAdapter<T1, T2>>(
                new MapAdapter<T1, T2>());

        return adapter;
    };

    std::shared_ptr<StageAdapter<T2, T1>> create_map_adapter(T2*(*map)(T1*)) {
        auto adapter = std::shared_ptr<StageAdapter<T2, T1>>(
                new MapAdapter<T2, T1>());

        return adapter;
    };
};

template <class... Ts> struct tuple {};

template <class T, class... Ts>
struct tuple<T, Ts...> : tuple<Ts...> {
    tuple(T t, Ts... ts) : tuple<Ts...>(ts...), tail(t) {}

    T tail;
};

template <size_t, class> struct elem_type_holder;

template <class T, class... Ts>
struct elem_type_holder<0, tuple<T, Ts...>> {
typedef T type;
};

template <size_t k, class T, class... Ts>
struct elem_type_holder<k, tuple<T, Ts...>> {
typedef typename elem_type_holder<k - 1, tuple<Ts...>>::type type;
};

template <size_t k, class... Ts>
typename std::enable_if<
        k == 0, typename elem_type_holder<0, tuple<Ts...>>::type&>::type
get(tuple<Ts...>& t) {
    return t.tail;
}

template <size_t k, class T, class... Ts>
typename std::enable_if<
        k != 0, typename elem_type_holder<k, tuple<T, Ts...>>::type&>::type
get(tuple<T, Ts...>& t) {
    tuple<Ts...>& base = t;
    return get<k - 1>(base);
}

unsigned int map_conditional_jump_to_start(LinearPipeline *lp) {
    if (lp->pipeline_running) {
        return 0;
    }

    return 1;
}

#endif //TASKFLOW_BUILDING_BLOCKS_HPP
