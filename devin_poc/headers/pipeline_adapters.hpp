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

#include <example_task_funcs.hpp>

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

    class BatchingVisitor : public boost::static_visitor<> {
    public:
        DataVariant v;

        template<typename VariantType>
        void operator()(VariantType &data) const {

        }
    };

    /*template<>
    void QDispatchVisitor::operator()(sentinel &queue) const {
        std::cout << "Visitor found a sentinel! This is unexpected" << std::endl;
    }*/

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

        virtual void dispatch() = 0;

        virtual void add_input(std::string input_name, EdgePtr input) = 0;

        virtual void add_subscriber(std::string sub_name, EdgePtr sub) = 0;

        virtual unsigned int queue_size() = 0;
    };

    class StageAdapterExt : public StageAdapterBaseExt {
    public:
        unsigned int type_id = 0;
        unsigned int input_limit = 1;

        std::string name;
        std::vector<std::string> input_names;
        std::vector<std::string> subscriber_names;
        std::vector <EdgePtr> inputs;
        std::vector <EdgePtr> subscribers;

        std::atomic<unsigned int> running;
        std::atomic<unsigned int> initialized;

        unsigned int processed = 0;
        unsigned int input_buffer_size = 1;
        unsigned int output_buffer_size = 1;
        unsigned int max_queue_size = 6 * (2 << 15);
        unsigned int read_count = 0;

        ~StageAdapterExt() = default;

        StageAdapterExt() : running{0}, initialized{0}, inputs{}, subscribers{}, input_names{}, subscriber_names{} {

        };

        void add_input(std::string input_name, EdgePtr input) override {
            if (input_limit > 0 and (inputs.size() == input_limit)) {
                std::stringstream sstream;
                sstream << "Adapter: [" << name << "] does not support more than " <<  input_limit << " input(s).\n";
                std::cerr << sstream.str();

                throw(sstream.str());
            }

            // TODO: check for attempts to add same edge more than once

            inputs.push_back(input);
        };

        void add_subscriber(std::string sub_name, EdgePtr sub) override {
            subscriber_names.push_back(sub_name);
            subscribers.push_back(sub);
        };

        void init() override {};

        void pump() override {};

        void dispatch() override {};

        unsigned int queue_size() override { return 0; }
    };

    class BatchAdapterExt : public StageAdapterExt {
    public:
        unsigned int batch_size;
        unsigned int timeout_ms;

        BatchAdapterExt(unsigned int batch_size, unsigned int timeout_ms) : batch_size{batch_size},
            timeout_ms{timeout_ms}, StageAdapterExt() {};

        void pump() override {
            // Pump shouldn't use any mutable shared state
            unsigned int read_count;
            DataVariant data_buffer[batch_size];
            DataVariant output_buffer;
            std::vector<PrimitiveVariant> batch;

            auto in = inputs[0];
            while (this->running == 1) {
                read_count = in->wait_dequeue_bulk_timed(&data_buffer[0], batch_size, std::chrono::milliseconds(10));
                if (read_count > 0) {
                    break;
                }

                boost::this_fiber::yield();
            };

            for (int i = 0; i < read_count; i++) {
                //batch.push_back(data_buffer[i]);
            }

            std::stringstream sstream;
            sstream << "[" << this->name << "] Got Input Pack" << std::endl;
            std::cout << sstream.str();

            output_buffer = batch;
            for (auto sub = this->subscribers.begin(); sub != this->subscribers.end(); sub++) {
                //std::cout << type_name<decltype((*sub).get())>() << std::endl;
                //std::cout << type_name<decltype(data_buffer)>() << std::endl;
                //boost::apply_visitor(dispatch_visitor, *(*sub).get());
                while (this->running and
                       not (*sub)->try_enqueue_bulk(&output_buffer, this->input_buffer_size)) {
                    boost::this_fiber::yield();
                };
            }

            this->processed++;
        }
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

    class MapVisitor: public boost::static_visitor<DataVariant> {
    public:
        std::function<PrimitiveVariant(PrimitiveVariant)> map;

        MapVisitor(std::function<PrimitiveVariant(PrimitiveVariant)> map) : map{map} {};

        DataVariant operator()(PrimitiveVariant &v) const {
            DataVariant x = map(v);
            return x;
        }

        DataVariant operator()(std::vector<PrimitiveVariant> &v_vec) const {
            std::vector<PrimitiveVariant> ret;
            for(auto it = v_vec.begin(); it != v_vec.end(); it++) {
                ret.push_back(map(*it));
            }

            DataVariant d = ret;
            return d;
        }
    };

    class MapAdapterExt : public StageAdapterExt {
    public:
        MapAdapterExt (MapVisitor visitor) : visitor { visitor }, StageAdapterExt() {};

         MapVisitor visitor;

        void pump() override {
            DataVariant data_buffer;
            unsigned int read_count;

            auto in = inputs[0];
            while (this->running == 1) {
                read_count = in->wait_dequeue_bulk_timed(&data_buffer, this->input_buffer_size,
                                                                  std::chrono::milliseconds(10));
                if (read_count > 0) { break; }

                boost::this_fiber::yield();
            };

            std::stringstream sstream;
            sstream << "[" << this->name << "] Got Input Pack" << std::endl;
            std::cout << sstream.str();

            data_buffer = boost::apply_visitor(visitor, data_buffer);

            for (auto sub = this->subscribers.begin(); sub != this->subscribers.end(); sub++) {
                //std::cout << type_name<decltype((*sub).get())>() << std::endl;
                //std::cout << type_name<decltype(data_buffer)>() << std::endl;
                //boost::apply_visitor(dispatch_visitor, *(*sub).get());
                while (this->running and not(*sub)->try_enqueue_bulk(&data_buffer, this->input_buffer_size)) {
                    boost::this_fiber::yield();
                };
            }

            this->processed++;
        }
    };

    class FlatMapVisitor: public boost::static_visitor<std::vector<DataVariant>> {
    public:
        std::function<std::vector<PrimitiveVariant>(PrimitiveVariant)> flat_map;

        FlatMapVisitor(std::function<std::vector<PrimitiveVariant>(PrimitiveVariant)> flat_map) :
            flat_map{flat_map} {};

        std::vector<DataVariant> up_convert(std::vector<PrimitiveVariant> v) const {
            std::vector<DataVariant> ret;

            for (auto it = v.begin(); it != v.end(); it++) {
                ret.push_back(*it);
            }

            return ret;
        }

        // Unpacked types from DataVariant: PrimitiveVariant or std::vector<PrimitiveVariant>
        std::vector<DataVariant> operator()(PrimitiveVariant &v) const {
            return up_convert(flat_map(v));
        }

        std::vector<DataVariant> operator()(std::vector<PrimitiveVariant> &v_vec) const {
            std::vector<DataVariant> ret;

            for(auto it = v_vec.begin(); it != v_vec.end(); it++) {
                ret.push_back(flat_map(*it));
            }

            return ret;
        }
    };

    class FlatMapAdapterExt : public StageAdapterExt {
    public:
        FlatMapAdapterExt(FlatMapVisitor visitor) : visitor{visitor}, StageAdapterExt() {};

        FlatMapVisitor visitor;

        void pump() override {
            unsigned int read_count = 0;
            DataVariant data_buffer;
            std::vector<DataVariant> output_buffer;

            auto in = inputs[0];
            while (this->running == 1) {
                read_count = in->wait_dequeue_bulk_timed(&data_buffer, this->input_buffer_size,
                                                         std::chrono::milliseconds(10));
                if (read_count > 0) {
                    break;
                }

                boost::this_fiber::yield();
            };

            std::stringstream sstream;
            sstream << "[" << this->name << "] Got Input Pack" << std::endl;
            std::cout << sstream.str();

            output_buffer = boost::apply_visitor(visitor, data_buffer);

            for (auto data = output_buffer.begin(); data != output_buffer.end(); data++) {
                data_buffer = *data;
                for (auto sub = this->subscribers.begin(); sub != this->subscribers.end(); sub++) {
                    //std::cout << type_name<decltype((*sub).get())>() << std::endl;
                    //std::cout << type_name<decltype(data_buffer)>() << std::endl;
                    //boost::apply_visitor(dispatch_visitor, *(*sub).get());
                    while (this->running and
                           not(*sub)->try_enqueue_bulk(&data_buffer, this->input_buffer_size)) {
                        boost::this_fiber::yield();
                    };
                }
            }

            this->processed++;
        }
    };

    class FilterVisitor: public boost::static_visitor<bool> {
    public:
        std::function<bool(PrimitiveVariant)> filter;

        FilterVisitor(std::function<bool(PrimitiveVariant)> filter) : filter{filter} {};

        bool operator()(PrimitiveVariant &v) const {
            return filter(v);
        }

        bool operator()(std::vector<PrimitiveVariant> &v_vec) const {
            for(auto it = v_vec.begin(); it != v_vec.end(); it++) {
                // TODO: think more about filtering batches
                if (not filter(*it)) {
                    return false;
                }
            }

            return true;
        }
    };

    class FilterAdapterExt : public StageAdapterExt {
    public:
        FilterVisitor visitor;

        FilterAdapterExt(FilterVisitor visitor) : visitor{ visitor }, StageAdapterExt() {};

        void pump() override {
            // Pump shouldn't use any mutable shared state
            unsigned int read_count;
            DataVariant data_buffer;

            auto in = inputs[0];
            while (this->running == 1) {
                read_count = in->wait_dequeue_bulk_timed(&data_buffer, this->input_buffer_size,
                                                                  std::chrono::milliseconds(10));
                if (read_count > 0) {
                    break;
                }

                boost::this_fiber::yield();
            };

            std::stringstream sstream;
            sstream << "[" << this->name << "] Got Input Pack" << std::endl;
            std::cout << sstream.str();

            if (boost::apply_visitor(visitor, data_buffer)) {
                for (auto sub = this->subscribers.begin(); sub != this->subscribers.end(); sub++) {
                    //std::cout << type_name<decltype((*sub).get())>() << std::endl;
                    //std::cout << type_name<decltype(data_buffer)>() << std::endl;
                    //boost::apply_visitor(dispatch_visitor, *(*sub).get());
                    while (this->running and
                           not (*sub)->try_enqueue_bulk(&data_buffer, this->input_buffer_size)) {
                        boost::this_fiber::yield();
                    };
                }
            }

            this->processed++;
        }
    };


    // TODO
    /*
    class UniqueAdapterExt : public StageAdapterExt {
    public:
        UniqueAdapterExt(unsigned int max_size) : max_history_size{max_size}, StageAdapterExt() {};

        unsigned int max_history_size = 1;
        std::vector <DataVariant> input_buffers;
        std::vector <DataVariant> history;

        void pump() override {
            // TODO: this probably isn't right. Filter shouldn't be applied to multiple input streams.
            //  Probably should check some criteria at a higher level based on number of input streams and
            //  subscribers
            for (auto in = this->inputs.begin(); in != this->inputs.end(); in++) {
                this->read_count = 0;
                while (this->running == 1) {
                    this->read_count = in->wait_dequeue_bulk_timed(&this->data_buffer, this->input_buffer_size,
                                                                      std::chrono::milliseconds(10));
                    if (this->read_count > 0) {
                        break;
                    }

                    boost::this_fiber::yield();
                };

                input_buffers.push_back(this->data_buffer);
            }
            std::stringstream sstream;
            sstream << "[" << this->name << "] Got Input Pack" << std::endl;
            std::cout << sstream.str();

            for (auto data = input_buffers.begin(); data != input_buffers.end(); data++) {
                this->data_buffer = *data;
                for (auto sub = this->subscribers.begin(); sub != this->subscribers.end(); sub++) {
                    //std::cout << type_name<decltype((*sub).get())>() << std::endl;
                    //std::cout << type_name<decltype(data_buffer)>() << std::endl;
                    //boost::apply_visitor(dispatch_visitor, *(*sub).get());
                    while (this->running and
                           not(*sub)->try_enqueue_bulk(&this->data_buffer, this->input_buffer_size)) {
                        boost::this_fiber::yield();
                    };
                }
            }
            input_buffers.clear();
        }
    };
     */

    class SinkVisitor: public boost::static_visitor<> {
    public:
        std::function<void(PrimitiveVariant)> sink;

        SinkVisitor(std::function<void(PrimitiveVariant)> sink) : sink{sink} {};

        void operator()(PrimitiveVariant &v) const {
            sink(v);
        }

        void operator()(std::vector<PrimitiveVariant> &v_vec) const {
            for(auto it = v_vec.begin(); it != v_vec.end(); it++) {
                sink(*it);
            }
        }
    };

    class SinkAdapterExt : public StageAdapterExt {
    public:
        SinkAdapterExt (SinkVisitor visitor) : visitor{ visitor },
            StageAdapterExt() {
            this->input_limit = 0; // We can sink any number of things
        };

        SinkVisitor visitor;

        void pump() override {
            unsigned int read_count;
            DataVariant data_buffer;
            std::vector<DataVariant> output_buffer;
            for (auto in = this->inputs.begin(); in != this->inputs.end(); in++) {
                while (this->running == 1) {
                    read_count = (*in)->wait_dequeue_bulk_timed(&data_buffer, this->input_buffer_size,
                                                                      std::chrono::milliseconds(10));
                    if (read_count > 0) {
                        break;
                    }

                    boost::this_fiber::yield();
                };
                // TODO
                boost::apply_visitor(visitor, data_buffer);
                //output_buffer.push_back(data_buffer);
            }

            std::stringstream sstream;
            sstream << "[" << this->name << "] Sinking Input Pack" << std::endl;
            std::cout << sstream.str();

            //boost::apply_visitor(visitor, output_buffer);

            this->processed++;
        }
    };



}
#endif //TASKFLOW_PIPELINE_ADAPTERS_HPP
