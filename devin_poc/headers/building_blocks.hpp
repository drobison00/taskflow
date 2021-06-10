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
};

// Specialize for specific input/output cases
template<>
void SourceAdapter<std::fstream, std::string>::pump() {
    std::string data;
    std::getline(source, data);
    output->enqueue(data);
    processed += 1;
    std::this_thread::sleep_for(std::chrono::seconds(1));
};

template<>
void SourceAdapter<std::fstream, std::string>::init() {
    source = std::fstream();
    source.open(connection_string);
};

typedef SourceAdapter<std::fstream, std::string> FileSourceAdapter;

template<class InputType>
class SinkThrowAway {
public:
    void pump(InputType input) {};
};

template<class InputType, class SinkType>
class SinkAdapter {
public:
    std::string connection_string;

    unsigned int processed;

    SinkType sink;
    BlockingConcurrentQueue <InputType> &input;

    SinkAdapter(std::string connection_string) :
            connection_string{connection_string}, processed{0} {};

    void init();

    void pump(InputType input);
};

// Specialize for specific input/output cases
template<class InputType, class SinkType>
void SinkAdapter<InputType, SinkType>::pump(InputType input) {
    sink.pump(input);
    processed += 1;
};

template<class InputType, class SinkType>
void SinkAdapter<InputType, SinkType>::init() {};

typedef SinkAdapter<json, SinkThrowAway<json>> DeleteSinkAdapter;

template<class InputType, class OutputType>
class StageAdapter {
public:
    unsigned int processed = 0;

    std::function<OutputType(InputType)> work_routine;
    using InputQueue = std::shared_ptr<BlockingConcurrentQueue<InputType>>;
    using OutputQueue = std::shared_ptr<BlockingConcurrentQueue<OutputType>>;

    InputQueue input;
    OutputQueue output;

    StageAdapter(std::function<OutputType(InputType)> work_routine, InputQueue input_edge) :
            work_routine{work_routine}, input{input_edge} {
        output = OutputQueue(new BlockingConcurrentQueue<OutputType>());
    };

    int pump();

    std::shared_ptr<BlockingConcurrentQueue <OutputType>> get_output_edge() {
        return output;
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


class LinearPipeline {
public:
    long int processed = 0;
    unsigned int stages = 0;
    std::atomic<unsigned int> pipeline_running = 0;
    std::vector<std::atomic < unsigned int>*> stage_running_flags;
    std::vector<std::shared_ptr<void>> stage_adapters;
    std::vector<std::shared_ptr<void>> edges;
    std::map<unsigned int, tf::Task> id_to_task_map;
    std::vector <tf::Task> task_chain;

    tf::Task init;
    tf::Taskflow pipeline;
    tf::Executor &service_executor;

    ~LinearPipeline() {
        while (!stage_running_flags.empty()) {
            for (int i = 0; i < stage_running_flags.size(); i++) {
                delete stage_running_flags[i];
            }
        }
    };

    LinearPipeline(tf::Executor &executor) : service_executor{executor} {
        init = pipeline.emplace([this]() {
            std::cout << "Initializing Pipeline" << std::endl;
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

    //template<class InputType, class SinkType>
    //void set_sink();
};

void LinearPipeline::add_conditional_stage(unsigned int (*cond_test)(LinearPipeline *)) {
    // TODO: this is entirely ad-hoc right now. It exists only as a way to add the jump to start
    // conditional. Still thinking through the architecture requirements to generalize.
    std::atomic<unsigned int> *run_flag = new std::atomic<unsigned int>(0);
    auto index = stage_running_flags.size();
    stage_running_flags.push_back(run_flag);
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
    std::shared_ptr<BlockingConcurrentQueue <InputType>> input_edge =
            std::static_pointer_cast<BlockingConcurrentQueue <InputType>>(edges[index - 1]);

    auto adapter = std::shared_ptr<StageAdapter<InputType, OutputType>>(
            new StageAdapter<InputType, OutputType>(work_routine, input_edge));

    stage_adapters.push_back(adapter);
    edges.push_back(adapter->get_output_edge());

    tf::Task stage_task = pipeline.emplace([this, index, adapter]() {
        static unsigned int sleep_time = 0;
        unsigned int a = 0, b = 1; // TODO should be class level enums
        if ((this->stage_running_flags[index])->compare_exchange_strong(a, b)) {
            std::cout << "Initializing stage " << index << " task." << std::endl;
            service_executor.async([&]() {
                while (this->pipeline_running == 1) { adapter->pump(); }
                this->stage_running_flags[index]->compare_exchange_strong(b, a);
            });
        }
        // Debugging prints, shows taskflow running independently of queue processing logic
        if (sleep_time > 0) {
            std::cout << "[Business Logic] Stage " << index << " has processed " << adapter->processed
                      << " tasks" << std::endl;

            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
        }
        sleep_time = 1;
    });

    id_to_task_map[index] = stage_task;
    task_chain.push_back(stage_task);
    task_chain[index - 1].precede(task_chain[index]);
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

    std::cout << "Adding pipeline source" << std::endl;
    tf::Task source_task = pipeline.emplace([this, adapter, index]() {
        static unsigned int sleep_time = 0;
        unsigned int a = 0, b = 1;

        if ((this->stage_running_flags[index])->compare_exchange_strong(a, b)) {
            std::cout << "Initializing source task." << std::endl;
            adapter->init();

            service_executor.async([&]() {
                while (this->pipeline_running == 1) { adapter->pump(); }
                this->stage_running_flags[index]->compare_exchange_strong(b, a);
            });
        }

        if (sleep_time > 0) {
            std::cout << "[Business Logic] Data source has pumped " << adapter->processed
                      << " tasks" << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
        }
        sleep_time = 1;
    }).name("source");

    id_to_task_map[index] = source_task;
    task_chain.push_back(source_task);
    init.precede(source_task);
};

/*template<class InputType, class SinkType>
void LinearPipeline::set_sink() {
    std::atomic<unsigned int> *run_flag = new std::atomic<unsigned int>(0);
    auto index = stage_running_flags.size();
    stage_running_flags.push_back(run_flag);

    auto input_edge = (BlockingConcurrentQueue <InputType> *) edges[index - 1];
    auto adapter = new SinkAdapter<InputType, SinkType>();

    stage_adapters.push_back((void *) &adapter);
    std::cout << "Adding pipeline sink" << std::endl;
    tf::Task sink_task = pipeline.emplace([this, &adapter, index]() {
        static unsigned int sleep_time = 0;
        unsigned int a = 0, b = 1;

        if ((this->stage_running_flags[index])->compare_exchange_strong(a, b)) {
            std::cout << "Initializing sink task." << std::endl;
            adapter.init();

            service_executor.async([&]() {
                while (this->pipeline_running == 1) { adapter.pump(); }
                this->stage_running_flags[index]->compare_exchange_strong(b, a);
            });
        }

        if (sleep_time > 0) {
            std::cout << "[Business Logic] Data sink has cleared " << adapter.processed
                      << " tasks" << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
        }
        sleep_time = 1;
    }).name("sink");

    id_to_task_map[index] = sink_task;
    task_chain.push_back(sink_task);
};*/

unsigned int work_routine_conditional_jump_to_start(LinearPipeline *lp) {
    return 0;
}

#endif //TASKFLOW_BUILDING_BLOCKS_HPP
