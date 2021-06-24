//
// Created by drobison on 6/22/21.
//
#include <batch.hpp>
#include <blockingconcurrentqueue.h>

#include <nlohmann/json.hpp>
#include <taskflow/taskflow.hpp>

#include <boost/fiber/all.hpp>
#include <boost/variant.hpp>

#ifndef TASKFLOW_ADAPTERS_HPP
#define TASKFLOW_ADAPTERS_HPP

template<typename T>
constexpr auto type_name2() noexcept {
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


using namespace moodycamel;
using namespace nlohmann;

class StageAdapterBase {
public:
    ~StageAdapterBase() = default;

    virtual void init() = 0;

    virtual void pump() = 0;

    virtual unsigned int queue_size() = 0;
};

// Class: MapItem
template <typename KeyT, typename ValueT>
class MapItem {

public:
    using KeyType = std::conditional_t <std::is_lvalue_reference_v<KeyT>, KeyT, std::decay_t<KeyT>>;
    using ValueType = std::conditional_t <std::is_lvalue_reference_v<ValueT>, ValueT, std::decay_t<ValueT>>;

    MapItem(KeyT&& k, ValueT&& v) : _key(std::forward<KeyT>(k)), _value(std::forward<ValueT>(v)) {}
    MapItem& operator = (const MapItem&) = delete;

    inline const KeyT& key() const { return _key; }
    inline const ValueT& value() const { return _value; }

    template <typename ArchiverT>
    auto save(ArchiverT & ar) const { return ar(_key, _value); }

    template <typename ArchiverT>
    auto load(ArchiverT & ar) { return ar(_key, _value); }

private:

    KeyType _key;
    ValueType _value;
};

// Function: make_kv_pair
template <typename KeyT, typename ValueT>
MapItem<KeyT, ValueT> make_kv_pair(KeyT&& k, ValueT&& v) {
    return { std::forward<KeyT>(k), std::forward<ValueT>(v) };
}


struct sentinel {};

typedef boost::variant<
std::shared_ptr<std::string>,
std::shared_ptr<json>,
std::shared_ptr<int>,
std::shared_ptr<double>
> EdgeData;

typedef boost::variant<
    BlockingConcurrentQueue <std::shared_ptr<std::string>>*,
    BlockingConcurrentQueue <std::shared_ptr<json>>*,
    BlockingConcurrentQueue <std::shared_ptr<int>>*,
    BlockingConcurrentQueue <std::shared_ptr<double>>*,
    sentinel
    > Edge;

class QDispatchVisitor: public boost::static_visitor<> {
public:
    EdgeData input_buffer;

    QDispatchVisitor(EdgeData input_buffer) : input_buffer{input_buffer} {};

    template <typename T>
    void operator()(T & queue) const {
        std::cout << "Visiting: " << type_name2< decltype(queue)>() << std::endl;
        //queue->try_enqueue_bulk(&input_buffer, 1);
    }
};

template<>
void QDispatchVisitor::operator()(sentinel &queue) const {
    std::cout << "Visitor found a sentinel! This is unexpected" << std::endl;
}


class StageAdapterExt : public StageAdapterBase {
public:
    std::string output_type;
    std::shared_ptr<Edge> output_buffer;
    std::vector<std::shared_ptr<Edge>> inputs;
    std::vector<std::shared_ptr<Edge>> subscribers;
    std::atomic<unsigned int> running;
    std::atomic<unsigned int> initialized;

    EdgeData data_buffer;
    QDispatchVisitor dispatch_visitor;

    unsigned int processed = 0;
    unsigned int input_buffer_size = 1;
    unsigned int output_buffer_size = 1;
    unsigned int max_queue_size = 6 * (2 << 15);

    ~StageAdapterExt() = default;

    StageAdapterExt(Edge *buffer) : running{0}, initialized{0}, inputs(), subscribers(),
                                    dispatch_visitor(data_buffer) {
        output_buffer = std::shared_ptr<Edge>(buffer);
    };

    void init() override {};

    void pump() override {};

    unsigned int queue_size() override { return 0; }
};

class FileSourceAdapterExt : public StageAdapterExt {
public:
    std::string connection_string;
    std::string data_debug; // TODO: only for testing
    std::fstream input_file_stream;

    FileSourceAdapterExt(Edge *edge) : StageAdapterExt(edge) {};

    void init() override final {
        input_file_stream = std::fstream();
        input_file_stream.open(connection_string);

        //TODO: For debugging
        std::getline(input_file_stream, data_debug);
    };

    void pump() override final {
        data_buffer = std::shared_ptr<std::string>(new std::string(data_debug));
        for (auto sub = this->subscribers.begin(); sub != this->subscribers.end(); sub++) {
            //std::cout << type_name2<decltype(*((*sub).get()))>() << std::endl;

            boost::apply_visitor(dispatch_visitor, *(*sub).get());
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }

        this->processed += 1;
    };

    unsigned int queue_size() override final {
        return 0;
        //return this->output_buffer->size_approx();
    };
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
