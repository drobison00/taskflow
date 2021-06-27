//
// Created by drobison on 6/23/21.
//
#include <pipeline_adapters.hpp>
#include <blockingconcurrentqueue.h>
#include <example_task_funcs.hpp>

#include <nlohmann/json.hpp>
#include <taskflow/taskflow.hpp>

#include <boost/fiber/all.hpp>

#ifndef TASKFLOW_PIPELINE_HPP
#define TASKFLOW_PIPELINE_HPP
using namespace moodycamel;
using namespace nlohmann;

namespace taskflow_pipeline {
    class Pipeline {
    public:
        enum StageType {
            op_custom, op_source, op_sink, op_filter, op_map, op_flatmap, op_batch, op_explode,
            op_conditional, op_unique
        };

        enum PipelineState {
            uninitialized, initialized, running
        };

        PipelineState pstate = uninitialized;
        std::string _name = "Pipeline";

        std::atomic<unsigned int> pipeline_running = 0;

        std::map <std::string, tf::Task> task_map;
        std::map <std::string, std::shared_ptr<StageAdapterExt>> adapter_map;
        std::vector <std::shared_ptr<StageAdapterExt>> adapter_launch_vec;

        tf::Taskflow pipeline;
        tf::Executor &service_executor;

        unsigned int fiber_thread_count;
        std::vector<std::thread> fiber_threadpool;
        boost::fibers::condition_variable_any fiber_pool_cond_signal;
        boost::fibers::mutex fiber_pool_mutex;

        ~Pipeline() {
            fiber_pool_cond_signal.notify_all();
            for (auto it = fiber_threadpool.begin(); it != fiber_threadpool.end(); it++) {
                (*it).join();
            }
        }

        Pipeline(tf::Executor &executor, unsigned int n_threads=-1) : fiber_pool_mutex{},
            fiber_pool_cond_signal{}, service_executor{executor} {
            task_map = std::map<std::string, tf::Task>();
            adapter_map = std::map < std::string, std::shared_ptr < StageAdapterExt >> ();

            pipeline.name(_name);

            auto init = pipeline.emplace([this]() {
            }).name("init");
            task_map[std::string("__init")] = init;

            auto shutdown = pipeline.emplace([]() {}).name("end");
            task_map[std::string("__end")] = shutdown;

            // Setup fiber threadpool.
            if (n_threads == -1) {
                n_threads = std::thread::hardware_concurrency();
            }
            n_threads = std::max(n_threads, (unsigned int)1);
            fiber_thread_count = 8;

            std::cout << "Creating threadpool with " << fiber_thread_count << " threads." << std::endl;

            boost::fibers::barrier fiber_init_barrier{fiber_thread_count + 1};
            for (int i = 0; i < fiber_thread_count; i++) {
                auto worker = std::thread([this, &fiber_init_barrier, i]() {
                    //boost::fibers::use_scheduling_algorithm<boost::fibers::algo::work_stealing>(10);
                    boost::fibers::use_scheduling_algorithm<boost::fibers::algo::shared_work>();
                    std::stringstream sstream;
                    sstream << "Fiber lead " << i << " waiting on barrier." << std::flush << std::endl;
                    std::cout << sstream.str();
                    sstream.str("");
                    fiber_init_barrier.wait();
                    this->fiber_pool_mutex.lock();
                    // mtx is unlocked on .wait, and will be acquired again before wait can return.
                    sstream << "Fiber lead " << i << " blocking." << std::flush << std::endl;
                    std::cout << sstream.str();
                    sstream.str("");
                    this->fiber_pool_cond_signal.wait(this->fiber_pool_mutex);
                    sstream << "Thread " << i << " terminating." << std::flush << std::endl;
                    std::cout << sstream.str();
                    std::cout << std::flush;
                    this->fiber_pool_mutex.unlock();
                });
                fiber_threadpool.push_back(std::move(worker));
            }
            fiber_init_barrier.wait();

            std::stringstream sstream;
            sstream << "Fiber pool initialized, " << fiber_threadpool.size() << " running threads\n";
            std::cout << sstream.str() << std::flush;
        }

        Pipeline &build() {
            service_executor.run(pipeline).wait();
            pstate = initialized;

            return *this;
        }

        Pipeline &start(unsigned int runtime) {
            pipeline_running = 1;
            //service_executor.run(pipeline).wait();
            std::thread([this](){
                //boost::fibers::use_scheduling_algorithm<boost::fibers::algo::work_stealing>(11);
                boost::fibers::use_scheduling_algorithm<boost::fibers::algo::shared_work>();
                for(auto it = this->adapter_launch_vec.rbegin();
                            it != this->adapter_launch_vec.rend(); it++) {
                    (*it)->init();
                    (*it)->run();
                }
            }).detach();

            unsigned int freq_ms = 33;
            std::stringstream sstream;
            auto adptr = adapter_map["source_input"];

            auto weight = 0.99;
            auto last_processed = adptr->processed;
            auto avg_throughput = 0.0;
            auto start_time = std::chrono::steady_clock::now();
            auto last_time = start_time;
            while (this->pipeline_running != 0) {
                auto cur_time = std::chrono::steady_clock::now();
                std::chrono::duration<double> elapsed = cur_time - start_time;
                std::chrono::duration<double> delta_t = cur_time - last_time;
                last_time = cur_time;

                auto processed = adptr->processed;
                auto delta_proc = processed - last_processed;
                avg_throughput = weight * avg_throughput + (1 - weight) * delta_proc / delta_t.count();
                sstream << std::setw(10) << std::setprecision(0) << std::fixed << avg_throughput <<
                " " << std::setw(10) << processed;

                sstream << " runtime: " << std::setw(4) << elapsed.count() << " sec\r";
                std::cout << sstream.str() << std::flush;
                last_processed = processed;
                std::this_thread::sleep_for(std::chrono::milliseconds(33));
            }

            std::this_thread::sleep_for(std::chrono::seconds(runtime));

            return *this;
        }

        Pipeline &name(std::string name) {
            name = name;
            pipeline.name(name);

            return *this;
        }

        Pipeline &wait() {
            return *this;
        }

        Pipeline &visualize(std::string filename) {
            std::ofstream file;
            file.open(filename);
            pipeline.dump(file);
            file.close();

            return *this;
        };

        Pipeline &add_node(std::shared_ptr <StageAdapterExt> adapter);

        Pipeline &batch(std::string &&task_name, std::vector<std::string> &&inputs,
                        unsigned int batch_size, unsigned int timeout_ms);

        Pipeline &filter(std::string &&task_name, std::vector <std::string> &&inputs,
                         std::function<bool(PrimitiveVariant)> func);

        Pipeline &filter(std::string &&task_name, std::vector <std::string> &&inputs,
                         FilterVisitor &&vistor);

        Pipeline &flat_map(std::string &&task_name, std::vector <std::string> &&inputs,
                           std::function<std::vector<PrimitiveVariant>(PrimitiveVariant)> func);

        Pipeline &flat_map(std::string &&task_name, std::vector <std::string> &&inputs,
                           FlatMapVisitor &&visitor);

        Pipeline &map(std::string &&task_name, std::vector <std::string> &&inputs,
                      std::function<PrimitiveVariant(PrimitiveVariant)> func);

        Pipeline &map(std::string &&task_name, std::vector <std::string> &&inputs,
                      MapVisitor &&visitor);


        Pipeline &sink(std::string &&task_name, std::vector <std::string> &&inputs,
                       std::function<void(PrimitiveVariant)> func);

        Pipeline &sink(std::string &&task_name, std::vector <std::string> &&inputs,
                       SinkVisitor &&visitor);

        Pipeline &source(std::string &&source_type, std::string &&connection_string,
                                   std::string &&task_name);

    private:
        void check_assert_task_exists(std::string task_name) {
            auto task_it = task_map.find(task_name);
            if (task_it != task_map.end()) {
                std::stringstream sstream;
                sstream << "Task ID: " << task_name << " already exists.";
                throw (sstream.str());
            }
        }
    };

    Pipeline &Pipeline::add_node(std::shared_ptr <StageAdapterExt> adapter) {
        check_assert_task_exists(adapter->name);
        adapter_map[adapter->name] = adapter;

        std::stringstream sstream;

        sstream << "[";
        switch (adapter->type_id) {
            case op_custom:
                sstream << "CUSTOM";
                break;
            case op_map:
                sstream << "MAP";
                break;
            case op_flatmap:
                sstream << "FLAT_MAP";
                break;
            case op_source:
                sstream << "SOURCE";
                break;
            case op_sink:
                sstream << "SINK";
                break;
            case op_filter:
                sstream << "FILTER";
                break;
            case op_unique:
                sstream << "UNIQUE";
                break;
            default:
                sstream << "UNKNOWN";
        }
        sstream << "]\n" << adapter->name;

        // This won't be run until later
        auto task = pipeline.emplace([this, adapter]() {
            if (this->pstate == uninitialized and adapter->initialized == 0) {
                for (auto input = adapter->input_names.begin(); input < adapter->input_names.end(); ++input) {
                    auto parent = this->adapter_map.find(*input);
                    if (parent == adapter_map.end()) {
                        std::stringstream sstream;
                        sstream << "Input ID: "
                                << *input << " does not exist at this point in graph construction "
                                << " Note: cycles are not currently supported.\n";
                        throw (sstream.str());
                    }
                    auto padapter = parent->second;

                    auto edge = std::shared_ptr < BlockingConcurrentQueue < DataVariant >> (
                            new BlockingConcurrentQueue<DataVariant>());

                    padapter->add_subscriber(*input, edge);
                    adapter->add_input(*input, edge);
                }
                this->adapter_launch_vec.push_back(adapter);
                adapter->initialized = 1;
            } /*
                // This doesn't seem to play well with boost fibers
                else if (this->pstate == initialized) {
                adapter->init();
                std::cout << "Running " << adapter->name << std::endl;
                adapter->run();
            }*/
        }).name(sstream.str());

        if (adapter->input_names.size() == 0) {
            auto parent = this->task_map.find("__init");
            auto ptask = parent->second;

            ptask.precede(task);
        }

        if (adapter->type_id == StageType::op_sink) {
            auto end = this->task_map.find("__end");
            auto etask = end->second;

            task.precede(etask);
        }

        for (auto input = adapter->input_names.begin(); input < adapter->input_names.end(); ++input) {
            auto parent = this->task_map.find(*input);
            if (parent == task_map.end()) {
                throw ("Cycles are not currently supported");
            }

            auto ptask = parent->second;
            ptask.precede(task);
        }

        task_map[adapter->name] = task;

        return *this;
    };

    Pipeline &Pipeline::batch(std::string &&task_name, std::vector<std::string> &&inputs,
        unsigned int batch_size, unsigned int timeout_ms) {
        check_assert_task_exists(task_name);

        auto adapter = std::shared_ptr<StageAdapterExt>(
                new BatchAdapterExt(batch_size, timeout_ms));
        adapter->name = task_name;
        adapter->input_names = inputs;
        adapter->type_id = StageType::op_batch;

        return add_node(adapter);
    }

    Pipeline &Pipeline::filter(std::string &&task_name, std::vector <std::string> &&inputs,
                     std::function<bool(PrimitiveVariant)> func) {
        return filter(std::move(task_name), std::move(inputs), FilterVisitor(func));
    }

    Pipeline &Pipeline::filter(std::string &&task_name,
                             std::vector <std::string> &&inputs,
                             FilterVisitor &&visitor) {

        check_assert_task_exists(task_name);

        auto adapter = std::shared_ptr<StageAdapterExt>(new FilterAdapterExt(visitor));
        adapter->name = task_name;
        adapter->input_names = inputs;
        adapter->type_id = StageType::op_filter;

        return add_node(adapter);
    }

    Pipeline &Pipeline::flat_map(std::string &&task_name, std::vector <std::string> &&inputs,
                                 std::function<std::vector<PrimitiveVariant>(PrimitiveVariant)> func) {

        return flat_map(std::move(task_name), std::move(inputs), FlatMapVisitor(func));
    }

    Pipeline &Pipeline::flat_map(std::string &&task_name,
                                 std::vector <std::string> &&inputs,
                                 FlatMapVisitor &&visitor) {

        check_assert_task_exists(task_name);

        auto adapter = std::shared_ptr<StageAdapterExt>(new FlatMapAdapterExt(visitor));
        adapter->name = task_name;
        adapter->input_names = inputs;
        adapter->type_id = StageType::op_flatmap;

        return add_node(adapter);
    }

    Pipeline &Pipeline::map(std::string &&task_name, std::vector <std::string> &&inputs,
                  std::function<PrimitiveVariant(PrimitiveVariant)> func) {

        return map(std::move(task_name), std::move(inputs), MapVisitor(func));
    }

    Pipeline &Pipeline::map(std::string &&task_name,
                            std::vector <std::string> &&inputs,
                            MapVisitor &&visitor) {

        check_assert_task_exists(task_name);

        auto adapter = std::shared_ptr<StageAdapterExt>(new MapAdapterExt(visitor));
        adapter->name = task_name;
        adapter->input_names = inputs;
        adapter->type_id = StageType::op_map;

        return add_node(adapter);
    }

    Pipeline &Pipeline::sink(std::string &&task_name,
                             std::vector <std::string> &&inputs,
                             std::function<void(PrimitiveVariant)> func) {
        return sink(std::move(task_name), std::move(inputs), SinkVisitor(func));
    }

    Pipeline &Pipeline::sink(std::string &&task_name,
                             std::vector <std::string> &&inputs,
                             SinkVisitor &&visitor) {

        check_assert_task_exists(task_name);

        auto adapter = std::shared_ptr<StageAdapterExt>(new SinkAdapterExt(visitor));
        adapter->name = task_name;
        adapter->input_names = inputs;
        adapter->type_id = StageType::op_sink;

        return add_node(adapter);
    }

    Pipeline &Pipeline::source(std::string &&source_type,
                               std::string &&connection_string,
                               std::string &&task_name) {

        check_assert_task_exists(task_name);
        std::shared_ptr <StageAdapterExt> adapter;
        if (source_type == "file") {
            adapter = std::shared_ptr<StageAdapterExt>(
                    new FileSourceAdapterExt(connection_string));
        } else {
            throw ("Unknown Source Adapter Type");
        }

        adapter->name = task_name;
        adapter->type_id = StageType::op_source;

        return add_node(adapter);
    }

}

#endif //TASKFLOW_PIPELINE_HPP
