//
// Created by drobison on 6/1/21.
//
// Create a control taskflow that pulls from a shared priority queue while a shared state variable is true. Each pulled
//  value is then passed to an asynchronous taskflow handler for execution.
//
#include <assert.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <string>

#include <blockingconcurrentqueue.h>

#include <nlohmann/json.hpp>
#include <taskflow/taskflow.hpp>  // Taskflow is header-only


using namespace std::chrono;
using json = nlohmann::json;

// Ideas: Cascading async TaskFlows (modules)
// Each module is launched by its parent using some batch size and fills it's child queues.
//
// Waterfall design that continually back-fills, which each phase executing if it has enough items otherwise falling back
int main(int argc, char **argv) {
    int rate_per_sec = 10;
    if (argc > 1) {
        rate_per_sec = std::stoi(argv[1]);
    }
    int timeout = 300;
    if (argc > 2) {
        timeout = std::stoi(argv[2]);
    }
    int running = 1;

    std::atomic<int> stage_1_running(0);
    std::atomic<int> stage_2_running(0);
    std::atomic<int> stage_3_running(0);

    long int processed = 0;
    int workers = 16;

    uint32_t buffer_size = 2 << 21;
    uint32_t chunk_size = 2 << 15;

    moodycamel::BlockingConcurrentQueue <std::string> stage_1_queue;
    moodycamel::BlockingConcurrentQueue <json> stage_2_queue;
    moodycamel::BlockingConcurrentQueue <json> stage_3_queue;
    moodycamel::BlockingConcurrentQueue <json> stage_1_remote_queue;

    int stage_1_count = 0;
    int stage_1_start = 0;
    int stage_2_count = 0;
    int stage_2_start = 0;
    int stage_1_stride = 100;
    int stage_2_stride = 1;

    std::srand(std::time(nullptr));
    tf::Executor executor(workers), executor_remote(2);
    tf::Taskflow taskflow, taskflow_remote;

    taskflow.name("Processing Pipeline");
    taskflow_remote.name("Remote Socket Pipeline");
    tf::Taskflow stage_1_subflow, stage_2_subflow;

    // Launch thread that runs for a fixed length of time and then sets the exit flag.
    std::thread timeout_thread([&running, timeout]() {
        std::cout << "Starting timeout thread\n";
        std::this_thread::sleep_for(std::chrono::seconds(timeout));
        running = 0;
        std::cout << "Stopping timeout thread\n";
    });

    // Periodically print calculated throughput and source queue size.
    std::thread throughput_compute_thread([&]() {
        int pstart, freq_ms = 33;
        double avg = 0.0, avg_q1 = 0.0, avg_q2 = 0.0, avg_q3 = 0.0, scale_factor = 1000 / freq_ms;
        std::stringstream sstream;

        sstream << "Average throughput: " << avg << " records/sec";
        auto start_time = steady_clock::now();
        while (running != 0) {
            pstart = processed;
            std::this_thread::sleep_for(milliseconds(freq_ms));
            avg = (avg * 0.99 + (processed - pstart) * scale_factor * 0.01);
            avg_q1 = (avg_q1 * 0.99 + (stage_1_queue.size_approx()) * 0.01);
            avg_q2 = (avg_q2 * 0.99 + (stage_2_queue.size_approx()) * 0.01);
            avg_q2 = (avg_q3 * 0.99 + (stage_3_queue.size_approx()) * 0.01);
            auto cur_time = steady_clock::now();
            duration<double> elapsed = cur_time - start_time;

            sstream.str("");
            sstream << "Average throughput: " <<
                    std::setw(15) << avg << " records/sec" <<
                    " avg.q1: " << std::setw(8) << avg_q1 <<
                    " avg.q2: " << std::setw(8) << avg_q2 <<
                    " avg.q3: " << std::setw(8) << avg_q3 <<
                    " processed: " << std::setw(10) << processed <<
                    " runtime: " << std::setw(8) << elapsed.count() << " sec.\r";
            std::cout << sstream.str() << std::flush;
        }
        std::cout << "Stopping throughput thread." << std::endl;
    });

    // Alternate taskflow to simulate a remote service waiting to receive socket data.
    tf::Task init_remote = taskflow_remote.emplace([&]() {
        std::cout << "Starting Remote Control Flow" << std::endl;

        executor.async([&]() {
            int server_fd, new_socket, readsz;
            struct sockaddr_in address;
            int opt = 1;
            int addrlen = sizeof(address);
            char buf[4096] = {0};

            if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
                std::cout << "Failed to grab socket" << server_fd << std::endl;
                running = 0;
            }

            if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
                std::cout << "Failed to set socket opts" << std::endl;
                running = 0;
            }
            address.sin_family = AF_INET;
            address.sin_addr.s_addr = INADDR_ANY;
            address.sin_port = htons(12345);

            if (bind(server_fd, (struct sockaddr *) &address, sizeof(address)) < 0) {
                std::cout << "Failed to bind server_fd" << std::endl;
                running = 0;
            }

            if (listen(server_fd, 3) < 0) {
                std::cout << "Failed to listen" << std::endl;
                running = 0;
            }

            while (running) {
                if ((new_socket = accept(server_fd, (struct sockaddr *) &address, (socklen_t * ) & addrlen)) < 0) {
                    std::cout << "Failed to create new socket" << std::endl;
                }

                int flags = fcntl(new_socket, F_GETFL, 0);
                flags = (flags | O_NONBLOCK);
                fcntl(new_socket, F_SETFL, flags);

                std::cout << "Got a new connection" << std::endl;

                struct pollfd fds[1];
                memset(fds, 0, sizeof(fds));
                auto ptimeout = 10000;

                fds[0].fd = new_socket;
                std::string s("");

                while (running) {
                    fds[0].fd = new_socket;
                    fds[0].events = POLLIN;

                    int error = 0;
                    socklen_t len = sizeof(error);
                    int retval = getsockopt(new_socket, SOL_SOCKET, SO_ERROR, &error, &len);

                    if (retval || error) {
                        fprintf(stderr, "error getting socket error code: %s\n", strerror(retval));
                        fprintf(stderr, "socket error: %s\n", strerror(error));
                    }

                    if (poll(fds, 1, ptimeout)) {
                        if (fds[0].revents & (POLLNVAL | POLLERR | POLLHUP)) {
                            std::cout << "Something terrible happened" << std::endl << std::flush;
                            shutdown(new_socket, SHUT_RD);
                            close(new_socket);
                            break;
                        }

                        if (fds[0].revents & POLLIN) {
                            readsz = recv(new_socket, buf, 4096, 0);
                            for (int i = 0; i < readsz; i++) {
                                s += buf[i];
                                if (buf[i] == '\0') {
                                    auto parsed_data = json::parse(s);
                                    // Don't push anything yet, because we're not doing anything with it.
                                    //stage_1_ring_buffer_remote.push(s);
                                    s.clear();
                                }
                            }
                        }
                    }
                }
            }

            return 0;
        });
    }).name("Remote Socket Reader");
    executor.run(taskflow_remote);
    // Hack for my bad socket implementation
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Socket connection to remote taskflow
    int _sock = 0;
    struct sockaddr_in _serv_addr;

    if ((_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        std::cout << "Failed to open connection to remote service" << std::endl;
    }

    _serv_addr.sin_family = AF_INET;
    _serv_addr.sin_port = htons(12345);
    if (inet_pton(AF_INET, "127.0.0.1", &_serv_addr.sin_addr) <= 0) {
        std::cout << "Invalid address" << std::endl;
    }

    if (connect(_sock, (struct sockaddr *) &_serv_addr, sizeof(_serv_addr)) < 0) {
        std::cout << "Connection failed" << std::endl;
    }

    /* TaskFlow setup */
    // TODO: Init should connect to our file, and pass an iterator forward?
    tf::Task init = taskflow.emplace([&]() {
        // this should return an iterator to our file/kafka object
        std::cout << "Starting Control Flow\n";

        // Simple dynamic task creation -- strided iteration loops.

        stage_1_subflow.name("Work phase 1");

        stage_2_subflow.for_each_index(std::ref(stage_2_start), std::ref(stage_2_count), std::ref(stage_2_stride),
                                       [&](auto x) {
                                           std::stringstream sstream;

                                           // Do some arbitrary JSON operations.
                                           bool found;
                                           for (int k = 0; k < stage_2_stride; k++) {
                                               json j;
                                               found = stage_2_queue.try_dequeue(j);
                                               if (not found) { break; }

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
                                               stage_3_queue.enqueue(j);
                                           }
                                       }).name("Stage2 for_each_index");
        stage_2_subflow.name("Work phase 2");

        std::ofstream file;
        file.open("subflow1.dot");
        stage_1_subflow.dump(file);
        file.close();

        file.open("subflow2.dot");
        stage_2_subflow.dump(file);
        file.close();

        // Sample Async execution, injects data into our pipeline
        executor.async([&]() {
            std::fstream json_file;
            json_file.open("devin_poc/without_data_len.json", std::ios::in);
            std::string json_data;
            std::getline(json_file, json_data);
            json_file.close();

            int count = 0;
            while (running) {
                stage_1_queue.enqueue(json_data);
                count++;

                if (count > rate_per_sec) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    count = 0;
                }
            }

            return 0;
        });
    }).name("init");

    // Stage one grabs available available work from the stage 1 buffer, and runs subflow 1 on it, which
    // pre-processes the json string into a 'json' object and puts it on buffer 2
    tf::Task stage_1 = taskflow.emplace([&]() {
        // Do any stage specific work/tuning here.
        int a = 0, b = 1;
        static std::string s[10000];
        static json j[1000];

        if (stage_1_running.compare_exchange_strong(a, b)) {
            std::cout << "Starting stage_1 async worker" << std::endl;
            stage_1_subflow.for_each_index(std::ref(stage_1_start), std::ref(stage_1_count), std::ref(stage_1_stride),
                                           [&](int x) {
                                               for (int i = x; i < x + stage_1_stride && i < stage_1_count; i++) {
                                                   auto k = i % 1000;
                                                   j[k] = json::parse(s[i]);
                                               }
                                           }).name("Stage_1 for_each_index");

            executor.async([&]() {
                size_t count;
                // try to allocate memory

                while (running) {
                    count = stage_1_queue.wait_dequeue_bulk_timed(&s[0], 10000,
                                                                  std::chrono::milliseconds(10));

                    for (int i = 0; i < count; i += 1000) {
                        stage_1_start = i;
                        auto batch_size = std::min<int>(1000, count - i);
                        stage_1_count = i + batch_size;

                        if (batch_size > 0) {
                            executor.run(stage_1_subflow).get();
                            stage_2_queue.enqueue_bulk(&j[0], batch_size);
                        }
                    }
                }
                stage_1_running.compare_exchange_strong(b, a);
            });
        }
    }).name("stage_1");

    // Stage 1 conditional, determines if we fall back to stage 1 or go forward to stage 2
    tf::Task stage_1_return = taskflow.emplace([&]() {
        if (not running) { return 2; }

        if (stage_2_queue.size_approx() > 0) { return 1; }

        return 0;
    }).name("stage_1_cond");

    tf::Task stage_2 = taskflow.emplace([&]() {
        int a = 0, b = 1;
        static json j[1000];

        if (stage_2_running.compare_exchange_strong(a, b)) {
            std::cout << "Starting stage_2 async worker" << std::endl;

            executor.async([&]() {
                size_t count;
                while (running) {
                    count = stage_2_queue.wait_dequeue_bulk_timed(&j[0], 1000,
                                                                  std::chrono::milliseconds(10));

                    for (int i = 0; i < count; i++) {
                        j[i]["some field"] = "some value";
                        j[i]["some list"] = {'a', 'b', 'c'};
                        j[i]["some dict"] = {{"thing1", 1},
                                             {"thing2", 3.1411111}};
                        if (j[i]["timestamp"] > 1616381017606) {
                            j[i]["is_first"] = false;
                        } else {
                            j[i]["is_first"] = true;
                            j[i].erase("flags");
                        }

                        // search the string
                        if (j[i].find("dest_port") != j[i].end()) {
                            if (j[i]["dest_port"] == "80") {
                                j[i]["is_http_dest"] = true;
                            } else {
                                j[i]["is_http_dest"] = false;
                            }
                        }
                    }

                    if (count > 0) {
                        stage_3_queue.enqueue_bulk(&j[0], count);
                    }
                }
                stage_2_running.compare_exchange_strong(b, a);
            });
        }
    }).name("stage 2");

    tf::Task stage_2_return = taskflow.emplace([&]() {
        if (not running) { return 2; }

        if (stage_3_queue.size_approx() > 0) { return 1; }

        return 0;
    }).name("stage_2_cond");

    tf::Task stage_3 = taskflow.emplace([&]() {
        int a = 0, b = 1;
        static json j[1000];

        if (stage_3_running.compare_exchange_strong(a, b)) {
            std::cout << "Starting stage_3 async worker" << std::endl;

            executor.async([&]() {
                int slen, sent;
                char *cstr;

                size_t count;
                while (running) {
                    count = stage_3_queue.wait_dequeue_bulk_timed(&j[0], 1000,
                                                                  std::chrono::milliseconds(10));

                    for (int i = 0; i < count; i++) {
                        std::stringstream sstream;
                        sstream << j[i];

                        slen = sstream.str().size();
                        cstr = new char[slen + 1]{0};

                        std::strcpy(cstr, sstream.str().c_str());
                        sent = send(_sock, cstr, slen + 1, 0);

                        delete cstr;
                        processed += 1;
                    }
                }
                stage_3_running.compare_exchange_strong(b, a);
            });
        }
    }).name("stage_3");

    tf::Task stage_3_return = taskflow.emplace([]() {
        return 0;
    }).name("stage_3_cond");

    tf::Task stop = taskflow.emplace([&]() {
        running = false;
        std::cout << "\nStopping Control Flow\n";
    }).name("stop");

    // Primary pipeline starts with init
    init.precede(stage_1);

    stage_1.precede(stage_1_return);
    stage_1_return.precede(stage_1, stage_2, stop);

    stage_2.precede(stage_2_return);
    stage_2_return.precede(stage_1, stage_3);

    stage_3.precede(stage_3_return);
    stage_3_return.precede(stage_1);

    // Write out our taskflow graph
    std::ofstream file;
    file.open("main_graph.dot");
    taskflow.dump(file);
    file.close();

    executor.run(taskflow).wait();

    std::cout << "\n" << std::flush;
    std::cout << "Joining metrics thread" << std::endl;
    throughput_compute_thread.join();

    std::cout << "Joining timeout thread" << std::endl;
    timeout_thread.join();

    std::cout << std::endl;

    return 0;
}