## Quickstart

Composition workflow/testing

Note: definitely a work in progress, currently meant for discussion and design review.

The current LinearPipeline architecture is designed as a series of taskflow tasks:
- Tasks communicate work through the pipeline asynchronously via concurrent queues.
- Each queue has a single writer (its task) and a single reader (its consuming task)
- Pipeline semantics are based, loosely, around a functional work flow, where each stage takes in the data it operates
   on and emits new data, except filter and batch operations. The 'filter' adapter, which is guaranteed 
   to output exactly the same data type, unchanged, simply removes the data pointer from its input queue and passes
   it to its output queue; the 'batch' adapter is slightly different, in that it emits a newly allocated 'Batch'
   object containing elements of the same data type, unchanged, it copies up to N data pointers from its input queue
   wraps them with the batch object, and emits the batch. Its pretty fast in its current state, and straight 
   forward to reason about.
- Back-pressure is handled in a straight forward fashion using fixed queue sizes. If a tasks output queue is full, the
  task will wait until it is able to push to it.
- The pipeline currently supports 6 primary operations and implements a function chaining pattern to build up a pipeline.
  source should be first (no checking for this currently).
    - Operations:
        - **source** - HACKY - currently only supports a hacked 'read once' from a file and inject at line rate operation.
          TODO: Need to define a better way to initialize source adpaters.
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
        - **sink** - signature(void(\*myfunc)(InputType\*)) - will receive a pointer to its input
          data, and can do whatever is required to retire it from the pipeline; however, the data itself will be freed as soon as
          myfunc returns, so the pointer should not be stored or passed to another function.
          TODO: Need to define a better way to initialize sink adpaters.

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

    lp.source(new FileSourceAdapter(std::string("devin_poc/without_data_len.json"), rate_per_sec))
        .filter(filter_random_drop)                // Randomly drop 50% of packets
        .map(map_string_to_json)                   // Parse strings into JSON objects
        .explode<json>(exploder_duplicate)         // Duplicated every JSON object 10x
        .map(map_random_work_on_json_object)       // Perform various operations on each JSON object
        .map<json>(map_random_trig_work)           // Do intensive trig work and forward JSON packets
        .explode<json>(exploder_duplicate)         // Duplicate every JSON object 10x
        .filter(filter_random_drop)                // Randomly drop 50% of packets
        .map<json>(map_random_trig_work)           // Do intensive trig work and forward JSON packets
        .explode<json>(exploder_duplicate)         // Duplicated every JSON object 10x
        .batch(10, 10)                             // Batch 10 JSON objects at a time and forward
        .sink(sink_discard)                        // Sink all packets
        .add_conditional_stage(map_conditional_jump_to_start) // Taskflow loopback
        .visualize("main_graph.dot")
        .start(timeout);
```
It will also display basic stats.
```
// shows avg_throughput(avg_queue_size)
[Sourced] =>   1681(1007), [filter] =>    810(1007), [map] =>    847(1023), [explode] =>    745(1009), [map] =>   7626(1018), [map] =>   7721(1019), [explode] =>   7790(1005), [filter] =>  38435(999), [map] =>  38706(1009), [explode] =>  38890(987), [batch] => 397659(986), [sunk] =>  39981(  3), [cond],      0(  0),  runtime:   27 sec
```