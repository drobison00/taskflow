//
// Created by drobison on 6/24/21.
//
#include <batch.hpp>
#include <common.hpp>
#include <blockingconcurrentqueue.h>

#include <nlohmann/json.hpp>
#include <taskflow/taskflow.hpp>

#include <boost/fiber/all.hpp>
#include <boost/variant.hpp>

using namespace moodycamel;
using namespace nlohmann;

#ifndef TASKFLOW_PIPELINE_ADAPTERS_HPP
#define TASKFLOW_PIPELINE_ADAPTERS_HPP
// Class: MapItem
namespace taskflow_pipeline {
    template<typename KeyT, typename ValueT>
    class MapItem {

    public:
        using KeyType = std::conditional_t <std::is_lvalue_reference_v<KeyT>, KeyT, std::decay_t<KeyT>>;
        using ValueType = std::conditional_t <std::is_lvalue_reference_v<ValueT>, ValueT, std::decay_t<ValueT>>;

        MapItem(KeyT &&k, ValueT &&v) : _key(std::forward<KeyT>(k)), _value(std::forward<ValueT>(v)) {}

        MapItem &operator=(const MapItem &) = delete;

        inline const KeyT &key() const { return _key; }

        inline const ValueT &value() const { return _value; }

        template<typename ArchiverT>
        auto save(ArchiverT &ar) const { return ar(_key, _value); }

        template<typename ArchiverT>
        auto load(ArchiverT &ar) { return ar(_key, _value); }

    private:

        KeyType _key;
        ValueType _value;
    };

// Function: make_kv_pair
    template<typename KeyT, typename ValueT>
    MapItem<KeyT, ValueT> make_kv_pair(KeyT &&k, ValueT &&v) {
        return {std::forward<KeyT>(k), std::forward<ValueT>(v)};
    }

    class QDispatchVisitor : public boost::static_visitor<> {
    public:
        //DataVariant input_buffer;

        //QDispatchVisitor(DataVariant input_buffer) : input_buffer{input_buffer} {};

        template<typename T>
        void operator()(T &queue) const {
            std::cout << "Visiting: " << type_name<decltype(queue)>() << std::endl;
            //queue->try_enqueue_bulk(&input_buffer, 1);
        }
    };

    template<>
    void QDispatchVisitor::operator()(sentinel &queue) const {
        std::cout << "Visitor found a sentinel! This is unexpected" << std::endl;
    }

    /*
    class QParseToJsonVisitor : public boost:static_visitor<> {
    public:
        template<typename T>
        void operator()(std::string &
    };
    */



    class StageAdapterBaseExt {
    public:
        ~StageAdapterBaseExt() = default;

        virtual void init() = 0;

        virtual void pump() = 0;

        virtual unsigned int queue_size() = 0;
    };

    class StageAdapterExt : public StageAdapterBaseExt {
    public:
        unsigned int type_id = 0;
        std::string name;
        std::string output_type;
        std::vector<std::string> input_names;
        std::vector<std::string> subscriber_names;
        std::vector <std::shared_ptr<BlockingConcurrentQueue < DataVariant>>>
        inputs;
        std::vector <std::shared_ptr<BlockingConcurrentQueue < DataVariant>>>
        subscribers;

        std::atomic<unsigned int> running;
        std::atomic<unsigned int> initialized;

        DataVariant data_buffer;

        unsigned int processed = 0;
        unsigned int input_buffer_size = 1;
        unsigned int output_buffer_size = 1;
        unsigned int max_queue_size = 6 * (2 << 15);
        unsigned int read_count = 0;

        ~StageAdapterExt() = default;

        StageAdapterExt() : running{0}, initialized{0}, inputs{}, subscribers{}, input_names{}, subscriber_names{} {

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

        FileSourceAdapterExt(std::string connection_string) :
            connection_string{connection_string},StageAdapterExt() {};

        void init() override final {
            input_file_stream = std::fstream();
            input_file_stream.open(connection_string);

            //TODO: For debugging
            std::getline(input_file_stream, data_debug);
        };

        void pump() override final {
            auto ptr = std::shared_ptr<std::string>(new std::string(data_debug));
            DataVariant buffer = ptr;
            //this->read_count = this->input->wait_dequeue_bulk_timed(&data_buffer,
            //                                                        1, std::chrono::milliseconds(10));
            for (auto sub = this->subscribers.begin(); sub != this->subscribers.end(); sub++) {
                //std::cout << type_name<decltype((*sub).get())>() << std::endl;
                //std::cout << type_name<decltype(data_buffer)>() << std::endl;
                //boost::apply_visitor(dispatch_visitor, *(*sub).get());
                //std::cout << "Pushing: " << *ptr << std::endl;
                while (this->running and not(*sub)->try_enqueue_bulk(&buffer, this->input_buffer_size)) {
                    boost::this_fiber::yield();
                };
            }
            std::this_thread::sleep_for(std::chrono::seconds(5));

            this->processed += 1;
        };

        unsigned int queue_size() override final {
            //return this->output_buffer->size_approx();
            return 0;
        };
    };

    class MapAdapterExt : public StageAdapterExt {
    public:
        MapAdapterExt (DataVariant(*map)(std::vector <DataVariant>)) : map{ map },

        StageAdapterExt() {};

        std::function<DataVariant(std::vector < DataVariant > )> map;
        std::vector <DataVariant> input_buffers;

        void pump() override {
            DataVariant data_buffer;
            std::vector <DataVariant> buffers;
            for (auto in = this->inputs.begin(); in != this->inputs.end(); in++) {
                this->read_count = 0;
                DataVariant buffer;
                while (this->running == 1) {
                    this->read_count = (*in)->wait_dequeue_bulk_timed(&buffer, this->input_buffer_size,
                                                                      std::chrono::milliseconds(10));
                    if (this->read_count > 0) {
                        break;
                    }

                    boost::this_fiber::yield();
                };

                buffers.push_back(buffer);
            }
            std::stringstream sstream;
            sstream << "[" << this->name << "] Got Input Pack" << std::endl;
            std::cout << sstream.str();

            data_buffer = map(buffers);

            for (auto sub = this->subscribers.begin(); sub != this->subscribers.end(); sub++) {
                //std::cout << type_name<decltype((*sub).get())>() << std::endl;
                //std::cout << type_name<decltype(data_buffer)>() << std::endl;
                //boost::apply_visitor(dispatch_visitor, *(*sub).get());
                while (this->running and not(*sub)->try_enqueue_bulk(&data_buffer, this->input_buffer_size)) {
                    boost::this_fiber::yield();
                };
            }
        }
    };

    class SinkAdapterExt : public StageAdapterExt {
    public:
        SinkAdapterExt (void(*sink)(std::vector <DataVariant>)) : sink{ sink },

        StageAdapterExt() {};

        std::function<void(std::vector < DataVariant > )> sink;
        std::vector <DataVariant> input_buffers;

        void pump() override {
            DataVariant data_buffer;
            for (auto in = this->inputs.begin(); in != this->inputs.end(); in++) {
                this->read_count = 0;
                DataVariant buffer;
                while (this->running == 1) {
                    this->read_count = (*in)->wait_dequeue_bulk_timed(&buffer, this->input_buffer_size,
                                                                      std::chrono::milliseconds(10));
                    if (this->read_count > 0) {
                        break;
                    }

                    boost::this_fiber::yield();
                };

                input_buffers.push_back(buffer);
            }

            std::stringstream sstream;
            sstream << "[" << this->name << "] Sinking Input Pack" << std::endl;
            std::cout << sstream.str();

            sink(input_buffers);
            input_buffers.clear();
        }
    };
}
#endif //TASKFLOW_PIPELINE_ADAPTERS_HPP
