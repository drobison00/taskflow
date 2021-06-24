//
// Created by drobison on 6/22/21.
//
#include <batch.hpp>
#include <common.hpp>
#include <blockingconcurrentqueue.h>

#include <nlohmann/json.hpp>
#include <taskflow/taskflow.hpp>

#include <boost/fiber/all.hpp>
#include <boost/variant.hpp>

#ifndef TASKFLOW_ADAPTERS_HPP
#define TASKFLOW_ADAPTERS_HPP

using namespace moodycamel;
using namespace nlohmann;

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

    void pump() override { return; }

    unsigned int queue_size() override {
        return input->size_approx();
    }

    OutputQueue get_output_edge() {
        return output;
    }
};


class FileSourceAdapter : public StageAdapter<void, std::string> {
public:
    std::string connection_string;
    std::string data_debug; // TODO: only for testing
    std::fstream input_file_stream;

    FileSourceAdapter() : StageAdapter<void, std::string>() {};
    FileSourceAdapter(std::string connection_string) :
            connection_string{connection_string}, StageAdapter<void, std::string>() {};

    void init() override {
        input_file_stream = std::fstream();
        input_file_stream.open(connection_string);

        //TODO: For debugging
        std::getline(input_file_stream, data_debug);
    };

    void pump() override {
        this->output_buffer = std::shared_ptr<std::string>(new std::string(data_debug));

        while (this->running == 1 && not(this->output->try_enqueue_bulk(&this->output_buffer, 1))) {
            boost::this_fiber::yield();
        }

        this->processed += 1;
    };

    unsigned int queue_size() override {
        return this->output->size_approx();
    };
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
            boost::this_fiber::yield();
            //std::this_thread::sleep_for(std::chrono::nanoseconds(100));
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
                boost::this_fiber::yield();
                //std::this_thread::sleep_for(std::chrono::nanoseconds(100));
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
            boost::this_fiber::yield();
            //std::this_thread::sleep_for(std::chrono::nanoseconds(100));
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
            boost::this_fiber::yield();
            //std::this_thread::sleep_for(std::chrono::nanoseconds(100));
        }

        this->processed += this->read_count;
    }
};

#endif //TASKFLOW_ADAPTERS_HPP
