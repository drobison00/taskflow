//
// Created by drobison on 6/1/21.
//
// Create a control taskflow that pulls from a shared priority queue while a shared state variable is true. Each pulled
//  value is then passed to an asynchronous taskflow handler for execution.
//
#include <taskflow/taskflow.hpp>  // Taskflow is header-only


using namespace std::chrono;
std::mutex stage1_mutex;
std::mutex log_mutex;

void produce(std::queue<int> &pq, const int rate_per_sec, const int &running) {

    std::cout << "Producer starting\n";
    auto rps = rate_per_sec;

    duration<double> time_span, sleep_span;
    duration<double> time_span_max(1.0);

    while (running) {
        high_resolution_clock::time_point start = high_resolution_clock::now();
        for (int i = 0; i < rps && running; i++) {
            {
                std::lock_guard <std::mutex> lock(stage1_mutex);
                pq.push(int(std::rand() % 100));
            }
        }
        high_resolution_clock::time_point end = high_resolution_clock::now();
        time_span = duration_cast < duration < double >> (end - start);
        sleep_span = duration_cast < duration < double >> (time_span_max - (end - start));

        if (time_span > time_span_max) {
            std::cout << "\nAdjusting timespan" << std::endl;
            auto rps_old = rps;
            rps = rps * (1 / time_span.count()) * 0.95;
            std::cout << "Unable to satisfy specified rate (" << rps_old << " /sec), scaling to ("
                      << rps << " /sec)" << std::endl;
        } else {
            std::this_thread::sleep_for(sleep_span);
        }
    }
    std::cout << "Producer Exiting.\n";
};

int stage1_ops(int val) {
    for(int i=0; i < std::rand()%100; i++){
        val += 1;
    }

    return val;
}

// Ideas: Cascading async TaskFlows (modules)
// Each module is launched by its parent using some batch size and fills it's child queues.
//
// Waterfall design that continually back-fills, which each phase executing if it has enough items otherwise falling back


int main() {
    int rate_per_sec = 2000000;
    int timeout = 300;
    int running = 1;
    long int processed = 0;
    std::srand(std::time(nullptr));
    tf::Executor executor(2);
    tf::Taskflow taskflow;

    std::queue<int> pq;
    std::queue<int> stage1_buffer;
    std::queue<std::string> stage2_buffer;

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
    std::thread throughput_compute_thread([&running, &processed, &pq](){
        int pstart, freq_ms = 250;
        double avg = 0.0, avg_inq = 0.0, scale_factor = 1000 / freq_ms;
        std::stringstream sstream;

        sstream << "Average throughput: " << avg << " records/sec";
        while (running) {
            pstart = processed;
            std::this_thread::sleep_for(std::chrono::milliseconds(freq_ms));
            avg = (avg * 0.97 + (processed - pstart) * scale_factor * 0.03);
            avg_inq = (avg_inq * 0.95 + pq.size() * 0.05);

            sstream.str("");
            sstream << "Average throughput: " << std::setw(15) << avg << " records/sec,  average inqueue: " <<
                std::setw(10) << avg_inq << "\r";
            std::cout << sstream.str() << std::flush;
        }
    });

    /* TaskFlow setup */
    // TODO: Init should connect to our file, and pass an iterator forward?
    tf::Task init = taskflow.emplace([]() {
        // this should return an iterator to our file/kafka object
        std::cout << "Starting Control Flow\n";
    }).name("init");

    // creates a condition task that returns a random binary
    // Assumptino here is that this task is the ONLY reader of PQ, and we don't care about order.
    tf::Task data_reader = taskflow.emplace([&pq, &running]() {
        if (running) {
            std::lock_guard <std::mutex> lock(stage1_mutex);

            if (pq.empty()) { return 0; }
            return 1;
        }

        return 2;
    }).name("data_reader");

    tf::Task stage1 = taskflow.emplace([&pq, &stage1_buffer, &running](){
        int val;
        {
            std::lock_guard <std::mutex> lock(stage1_mutex);
            while (running && !pq.empty()) {
                stage1_buffer.push(pq.front());
                pq.pop();
                // need some kind of timeout to avoid starvation
            }
        }

        return 0;
    });

    tf::Task stage1_return = taskflow.emplace([&stage1_buffer](){
        static int threshold = 1;
        if (stage1_buffer.size() >= threshold) { return 1; }

        return 0;
    });

    tf::Task stage2 = taskflow.emplace([&stage1_buffer, &stage2_buffer, &processed, &running](){
        /*{
            std::lock_guard<std::mutex> lock(log_mutex);
            std::cout << "Stage 2 running with stage1_buffer size: " << stage1_buffer.size() << std::endl;
        }*/

        while (stage1_buffer.size() > 0 && running) {
            std::string s(10, stage1_buffer.front());
            stage1_buffer.pop();
            stage2_buffer.push(s);
        }
    });

    tf::Task stage2_return = taskflow.emplace([&stage2_buffer](){
        static int threshold = 1;
        if (stage2_buffer.size() >= threshold) { return 1; }

        return 0;
    });

    tf::Task stage3 = taskflow.emplace([&stage2_buffer, &processed, &running](){
        std::stringstream sstream;

        while (stage2_buffer.size() > 0 && running) {
            sstream.str("");
            for(int i = 0; i < 10 && stage2_buffer.size() > 0 && running; i++) {
                sstream << stage2_buffer.front() << " ";
                stage2_buffer.pop();
                processed += 1;
            }
        }
    });

    tf::Task stage3_return = taskflow.emplace([](){
       return 0;
    });

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

    stage2_return.succeed(stage2);
    stage2_return.precede(data_reader, stage3);

    stage3.precede(stage3_return);

    stage3_return.succeed(stage3);
    stage3_return.precede(data_reader);

    executor.run(taskflow).wait();
    std::cout << "\n" << std::flush;
    timeout_thread.join();
    throughput_compute_thread.join();
    producer_thread.join();

    return 0;
}