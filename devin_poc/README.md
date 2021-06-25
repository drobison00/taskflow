## Quickstart

Composition workflow/testing

Note: definitely a work in progress, currently meant for discussion and design review.

The current Pipeline architecture is designed around both an underlying taskflow graph, which is used to build and
    link a secondary meta graph consisting of Adapter nodes and Concurrent Queue edges at run time.

- Adapter's are added using the standard 'source', 'map', 'filter' operations, with support for function chaining.
- Each adapter needs to delcare its name, the published name of it's output, a list of N possible inputs, and (maybe)
  an operator function.
  ex. .map(\['map_node_1'\], \['map_1_output_name'\], \[{'input_1', 'input_2', ... , 'input_n'}\]
- All operator functions will operate on a std::vector of shared pointers to boost::variant types (DataVariant), and
  produce either a single DataVariant object, or a std::vector of DataVariant types. DataVariant encapsulates 
  shared pointer types for all valid data types that can flow through the pipeline.
- Initial calls to pipeline operators will register them with the pipeline and create an associated taskflow task that
  will be used at build time to connect adapters and edges, and start the pipeline.
- When 'build' is called, it will run the underlying taskflow pipeline, in an 'uninitialized' state; this will visit
  each node in the graph, initialize it's adapter, link all of its inputs and subscribers.
- After initialization the pipeline can be started with a call to 'start', which executes the taskflow graph again in
  an 'initialized' state; this will visit each task node in topological order, again, and launch a std::thread
  which will be responsible for managing that node's input/output links using boost::fibers.
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
        - **source** 
        - **batch** - signature (count, timeout) .. parity with **sliding_window**
        - **buffer** - Uncessary (?) back pressure is handled automatically.
        - **collect** - Use case isn't entirely clear, parity can be achieved with **batch**
        - **combine_latest** - (TODO)
        - **delay** - (TODO) parity with passthrough filter with delay
        - **filter** - Done
        - **flatten** - (TODO) - reverse batch
        - **map** - Done
        - **partition** - (TODO) parity with **batch**
        - **rate_limit** - (TODO) parity with a passthrough filter with stateful rate 
        - **sink** - signature(void(\*myfunc)(std::vector<DataVariant>)) - will receive a std::vecto of DataVariants
          and can do whatever is required to retire them from the pipeline.
        - **sliding_window** (TODO) parity with **batch** if we allow for windowing.
        - **union** (TODO) parity with map
        - **unique** (TODO) probably a stateful specialization of filter.
        - **pluck** (TODO) parity with filter
        - **zip** (TODO) parity with map

```
g++ -g -std=c++17 devin_poc/pipeline_example.cpp -I ./ -I ../json/include -I ../concurrentqueue -I devin_poc/headers/ -lboost_fiber -lboost_context -O2 -pthread -o taskflow_test
```

Note: the current code will pump a json string into the pipeline every 5 seconds, which makes debugging/analysis easier
to reason about in development. Removing the sleep in the FileSourceAdapter.pump() function will allow the pipeline to
run at its limit.
```
./taskflow_test [max source read rate] [timeout]
./taskflow_test 100000 300
```

The above command will initialize and run this example pipeline:

```c++
    Pipeline p(executor);
    p.source("file", "devin_poc/without_data_len.json", "source_input")
        .map("map_to_json_1", {"source_input"}, map_parse_to_json)
        .map("map_to_json_2", {"source_input"}, map_parse_to_json)
        .map("map_json_to_json_1", {"map_to_json_1", "map_to_json_2"}, map_merge_json)
        .map("map_json_to_json_2", {"map_to_json_2", "map_to_json_1"}, map_merge_json)
        .map("map_json_to_json_3",
        {"map_to_json_2", "map_json_to_json_1", "map_json_to_json_2"},
        map_merge_json)
        .map("map_json_to_json_4",
        {"map_to_json_1", "map_json_to_json_2", "map_to_json_2"},
        map_merge_json)
        .map("map_json_to_json_5",
        {"map_to_json_1", "map_json_to_json_2", "map_to_json_2"},
        map_merge_json)
        .map("map_json_to_json_6",
        {"map_to_json_1", "map_json_to_json_2", "map_to_json_2"},
        map_merge_json)
        .map("map_json_to_json_7",
        {"map_to_json_1", "map_json_to_json_2", "map_to_json_2"},
        map_merge_json)
        .sink("sink1", {"map_json_to_json_3", "map_json_to_json_5", "map_json_to_json_6"},
        sink_passthrough)
        .sink("sink2", {"map_json_to_json_7", "map_json_to_json_4"}, sink_passthrough)
        .build()
        .name("Example taskflow")
        .visualize("graph.dot")
        .start(timeout);
```