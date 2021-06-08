//
// Created by drobison on 6/1/21.
//
// Create a control taskflow that pulls from a shared priority queue while a shared state variable is true. Each pulled
//  value is then passed to an asynchronous taskflow handler for execution.
//
#include <taskflow/taskflow.hpp>  // Taskflow is header-only


using namespace std::chrono;

std::mutex pq_mutex;
std::mutex stage1_mutex;
std::mutex stage2_mutex;
std::mutex log_mutex;

void produce(std::queue<int> &pq, const int rate_per_sec, const int &running) {

    std::cout << "Producer starting\n";
    auto rps = rate_per_sec;

    duration<double> time_span, sleep_span;
    duration<double> time_span_max(1.0);

    while (running) {
        high_resolution_clock::time_point start = high_resolution_clock::now();
        for (int i = 0; i < rps && running; i++) {
            std::lock_guard <std::mutex> lock(pq_mutex);
            auto v = int(std::rand() % 100);
            pq.push(v);
        }
        high_resolution_clock::time_point end = high_resolution_clock::now();
        time_span = duration_cast < duration < double >> (end - start);
        sleep_span = duration_cast < duration < double >> (time_span_max - (end - start));

        if (time_span > time_span_max) {
            auto rps_old = rps;
            rps = rps * (1 / time_span.count()) * 0.95;
            std::cout << "Unable to satisfy specified prducer rate (" << rps_old << " /sec), scaling to ("
                      << rps << " /sec)" << std::endl;
        } else {
            std::this_thread::sleep_for(sleep_span);
        }
    }
    std::cout << "Producer Exiting.\n";
};

int stage1_ops(int val) {
    for (int i = 0; i < std::rand() % 100; i++) {
        val += 1;
    }

    return val;
}

// Ideas: Cascading async TaskFlows (modules)
// Each module is launched by its parent using some batch size and fills it's child queues.
//
// Waterfall design that continually back-fills, which each phase executing if it has enough items otherwise falling back
int main(int argc, char **argv) {
    int rate_per_sec = 10;
    if (argc > 1) {
        rate_per_sec = std::stoi(argv[1]);
    }
    int timeout = 10;
    int running = 1;
    long int processed = 0;
    int workers = 16;
    int stage1_size = 0;
    int index2_start = 0, index2_end = 0;
    int stage2_stride = 100;
    int index3_start = 0, index3_end = 0;
    int stage3_stride = 100;

    std::srand(std::time(nullptr));
    tf::Executor executor(workers);
    tf::Taskflow taskflow;
    taskflow.name("Processing Pipeline");
    tf::Taskflow stage1_subflow, stage2_subflow;

    std::queue<int> pq;
    std::vector<int> stage1_buffer;
    std::vector <std::string> stage2_buffer;

    // Launch producer thread
    std::thread producer_thread(produce, std::ref(pq), rate_per_sec, std::ref(running));

    // Launch thread that runs for a fixed length of time and then sets the exit flag.
    std::thread timeout_thread([&running, timeout]() {
        std::cout << "Starting timeout thread\n";
        std::this_thread::sleep_for(std::chrono::seconds(timeout));
        running = 0;
        std::cout << "Stopping timeout thread\n";
    });

    // Periodically print calculated throughput and source queue size.
    std::thread throughput_compute_thread([&running, &processed, &pq]() {
        int pstart, freq_ms = 500;
        double avg = 0.0, avg_inq = 0.0, scale_factor = 1000 / freq_ms;
        std::stringstream sstream;

        sstream << "Average throughput: " << avg << " records/sec";
        while (running) {
            pstart = processed;
            std::this_thread::sleep_for(std::chrono::milliseconds(freq_ms));
            avg = (avg * 0.90 + (processed - pstart) * scale_factor * 0.1);
            {
                std::lock_guard <std::mutex> lock(pq_mutex);
                avg_inq = (avg_inq * 0.85 + pq.size() * 0.15);
            }

            sstream.str("");
            sstream << "Average throughput: " << std::setw(15) << avg << " records/sec,  average inqueue: " <<
                    std::setw(10) << avg_inq << " processed: " << std::setw(12) << processed << "\r";
            std::cout << sstream.str() << std::flush;
        }
    });

    /* TaskFlow setup */
    // TODO: Init should connect to our file, and pass an iterator forward?
    tf::Task init = taskflow.emplace([&]() {
        // this should return an iterator to our file/kafka object
        std::cout << "Starting Control Flow\n";

        stage1_buffer.reserve(5e6);
        stage2_buffer.reserve(5e6);
        std::cout << "Stage 1 capacity: " << stage1_buffer.capacity() << std::endl;
        std::cout << "Stage 2 capacity: " << stage2_buffer.capacity() << std::endl;

        stage1_subflow.name("Work phase 1");
        stage1_subflow.for_each_index(std::ref(index2_start), std::ref(index2_end), stage2_stride,
                                      [&](int i) {
                                          for (int k = i; k < i + stage2_stride && k < index2_end; k++) {
                                              auto op_result = stage1_ops(stage1_buffer[i]);
                                              std::string s(10, op_result);
                                              {
                                                  //std::lock_guard <std::mutex> lock(stage2_mutex);
                                                  //std::cout << "Worker ID: " << executor.this_worker_id() << " " << i
                                                  //          << std::endl << std::flush;
                                              }

                                              stage2_buffer[k] = s;
                                          }
                                      }).name("Stage1 for_each_index");

        stage2_subflow.name("Work phase 2");
        stage2_subflow.for_each_index(std::ref(index3_start), std::ref(index3_end), stage3_stride,
                                      [&](int i) {
                                          std::stringstream sstream;
                                          // Strided processing?
                                          for (int k = i; k < i + stage3_stride && k < index3_end; k++) {
                                              sstream << stage2_buffer[k] << " ";
                                          }
                                      }).name("Stage2 for_each_index");


        std::ofstream file;
        file.open("subflow1.dot");
        stage1_subflow.dump(file);
        file.close();

        file.open("subflow2.dot");
        stage2_subflow.dump(file);
        file.close();

    }).name("init");

    // creates a condition task that returns a random binary
    // Assumptino here is that this task is the ONLY reader of PQ, and we don't care about order.
    tf::Task data_reader = taskflow.emplace([&pq, &running]() {
        if (running) {
            std::lock_guard <std::mutex> lock(pq_mutex);
            if (pq.empty()) {
                return 0;
            }
            return 1;
        }

        return 2;
    }).name("data_reader");

    tf::Task stage1 = taskflow.emplace([&]() {
        int val;
        //std::cout << "stage 1 enter" << std::endl << std::flush;

        std::lock_guard <std::mutex> lock(pq_mutex);

        while (!pq.empty()) {
            stage1_buffer.push_back(pq.front());
            pq.pop();
            stage1_size += 1;
        }
        //std::cout << "stage 1 exit" << std::endl << std::flush;
    }).name("stage 1");

    tf::Task stage1_return = taskflow.emplace([&stage1_buffer]() {
        static int threshold = 1;

        //std::cout << "Stage1 return: " << stage1_buffer.capacity() << " : " << stage1_buffer.size() << std::endl;
        if (stage1_buffer.size() >= threshold) {
            return 1;
        }

        return 0;
    }).name("stage1_cond");

    tf::Task stage2 = taskflow.emplace([&]() {
        index2_start = 0;
        index2_end = stage1_size;
        executor.run(stage1_subflow).wait();

        // Index operator [] doesn't set size, we've got to keep track of it separately.
        index3_end = index2_end;

        //std::cout << "stage 2 exit" << std::endl << std::flush;
    }).name("stage2");

    tf::Task stage2_return = taskflow.emplace([&]() {
        static int threshold = 1;

        if (index3_end >= threshold) {
            return 1;
        }

        return 0;
    }).name("stage2_cond");

    tf::Task stage3 = taskflow.emplace([&]() {
        std::stringstream sstream;
        //std::cout << "Stage 3" << std::endl << std::flush;

        index3_start = 0;
        executor.run(stage2_subflow).wait();
        processed += index3_end;
        stage1_size -= index3_end;
        //std::cout << "stage 3 exit" << std::endl << std::flush;
    }).name("stage 3");

    tf::Task stage3_return = taskflow.emplace([]() {
        return 0;
    }).name("stage3_cond");

    // TODO: Stop could close our file and ensure the queue is drained correctly.
    tf::Task stop = taskflow.emplace([]() {
        std::cout << "\nStopping Control Flow\n";
    }).name("stop");


    // This will open whatever connection items we need
    init.precede(data_reader);

    // Read in some number of records at a time.
    data_reader.precede(data_reader, stage1, stop);

    // Stage 1 of n: just some fake work
    stage1.precede(stage1_return);

    // If we don't have enough to go forward, fallback to reader and cycle again.
    stage1_return.precede(data_reader, stage2);

    // Finished processing everything in the stage1 queue, fallback to reader
    stage2.precede(stage2_return);

    stage2_return.precede(data_reader, stage3);

    stage3.precede(stage3_return);

    stage3_return.precede(data_reader);

    std::ofstream file;
    file.open("main_graph.dot");
    taskflow.dump(file);
    file.close();

    executor.run(taskflow).wait();
    std::cout << "\n" << std::flush;
    timeout_thread.join();
    throughput_compute_thread.join();
    producer_thread.join();

    std::cout << std::endl;

    return 0;
}