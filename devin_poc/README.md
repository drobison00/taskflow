## Quickstart

Composition workflow/testing

Note: definitely a work in progress, currently meant for discussion and design review.

The current LinearPipeline architecture is designed as a series of taskflow tasks:
- Tasks communicate work through the pipeline asynchronously via concurrent queues.
- Each queue has a single writer (its task) and a single reader (its consuming task)
- Pipeline semantics are based, loosely, around a functional work flow, where each stage takes in the data it operates
   on and emits new data. There are likely optimizations to be made here, such as with filtering or map operations that
   produce a single output that is of the same type as the input; however, its pretty fast in its current state, and
   straight forward to reason about.
- Back-pressure is handled in a straight forward fashion using fixed queue sizes. If a tasks output queue is full, the
  task will wait until it is able to push to it.
- The pipeline currently supports 6 primary operations and implements a function chaining pattern to build up a pipeline.
  source should be first (no checking for this currently).
    - Operations:
        - **set\_source** - HACKY - currently only supports a hacked 'read once' from a file and inject at line rate operation.
        - **filter** - signature: filter(bool(\*myfunc)(InputType\*)). The function takes in a 
          pointer to a data element of type InputType, and returns 'true' if we keep the item and 'false' if we don't.
        - **map** - signature: map(OutputType*(\*myfunc)(InputType\*)). The function will receive 
          a pointer to a data element of type InputType, and must return a NEW function pointer of type OutputType. 
        - **explode** - signature: explode(std::tuple<DataType \*\*, unsigned int>(\*myfunc)(OutputType\*)) - Takes in a data element of
          type InputType* and must return a tuple containing an array of pointers to new OutputTypes, and the number of
          items created.
        - **batch** - signature batch(max_batch_size, timeout), collects up to max_batch_size elements, or less if 'timeout'
          and places a Batch<InputType>* object on its output queue.
          NOTE: If you add a batch function to the pipeline, subsequent maps/filters etc.. should expect to operate on a 
          Batch object.
        - **set_sink** - signature(std::string connection_string, void(\*myfunc)(InputType\*)) - will receive a pointer to its input
          data, and can do whatever is required to retire it from the pipeline; however, the data itself will be freed as soon as
          myfunc returns, so the pointer should not be stored or passed to another function.

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