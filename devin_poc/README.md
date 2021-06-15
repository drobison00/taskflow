## Quickstart

Composition workflow/testing


The current LinearPipeline architecture is designed as a series of taskflow tasks:
- Tasks communicate work through the pipeline asynchronously via concurrent queues.
- Each queue has a single writer (its task) and a single reader (its consuming task)
- Pipeline semantics are based, loosely, around a functional work flow, where each stage takes in the data it operates
   on and emits new data. There are likely optimizations to be made here, such as with filtering or map operations that
   produce a single output that is of the same type as the input; however, its pretty quick in its current state, and
   straight forward to reason about.
- The pipeline currently supports 6 primary operations and implements a function chaining pattern to build up a pipeline.
  source should be first (no checking for this currently).
    - Operations:
        - set_source:
        - filter:
        - map 
        - explode
        - batch
        - set_sink

```
g++ -g -std=c++17 devin_poc/composition_testing.cpp -I ./ -I ../json/include -I ../concurrentqueue -I devin_poc/headers/ -O2 -pthread -o taskflow_test
```

```
./taskflow_test [max source read rate] [timeout]
./taskflow_test 100000 300
```

The above command will initialize and run this example pipeline:

```c++
    tf::Executor executor(workers);
    LinearPipeline lp(executor, true);

    lp.set_source<std::fstream, std::string>(std::string("devin_poc/without_data_len.json"), rate_per_sec)
     .filter(filter_random_drop)                // Randomly drop 50% of packets
     .map(map_string_to_json)                   // Parse strings into JSON objects
     .map(map_random_work_on_json_object)       // Perform various operations on each JSON object
     .map(map_random_trig_work<json>)           // Do intensive trig work and forward JSON packets
     .explode(exploder_duplicate<json>)         // Duplicate every JSON object 10x
     .filter(filter_random_drop)                // Randomly drop 50% of packets
     .map(map_random_trig_work<json>)           // Do intensive trig work and forward JSON packets
     .explode(exploder_duplicate<json, json>)   // Duplicated every JSON object 10x
     .batch(10, 10)                             // Batch 10 JSON objects at a time and forward
     .set_sink(std::string(""), sink_discard)   // Sink all packets
     .add_conditional_stage(map_conditional_jump_to_start) // Taskflow loopback
     .visualize("main_graph.dot")
     .start(timeout);
```
It will also display basic stats.
```
// shows avg_throughput(avg_queue_size)
roughput msg/sec: [pumped] =>      19344(1007), [q.1] =>     9266(1007), [q.2] =>     9562(1022), [q.3] =>     9480(1021), [q.4] =>     9506(1018), [q.5] =>     9393(1004), [q.6] =>    47023(998), [q.7] =>    48155(1008), [q.8] =>    46864(880), [q.9] =>   485079(963), [cond]    runtime:        1 sec
```