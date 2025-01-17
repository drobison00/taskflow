#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN

#include <doctest.h>
#include <taskflow/taskflow.hpp>
#include <taskflow/cudaflow.hpp>

constexpr float eps = 0.0001f;

// --------------------------------------------------------
// Testcase: add2
// --------------------------------------------------------
template <typename T, typename F>
void add2() {

  //const unsigned N = 1<<20;
    
  tf::Taskflow taskflow;
  tf::Executor executor;

  for(size_t N=1; N<=(1<<20); N <<= 1) {

    taskflow.clear();

    T v1 = ::rand() % 100;
    T v2 = ::rand() % 100;

    std::vector<T> hx, hy;

    T* dx {nullptr};
    T* dy {nullptr};
    
    // allocate x
    auto allocate_x = taskflow.emplace([&]() {
      hx.resize(N, v1);
      REQUIRE(cudaMalloc(&dx, N*sizeof(T)) == cudaSuccess);
    }).name("allocate_x");

    // allocate y
    auto allocate_y = taskflow.emplace([&]() {
      hy.resize(N, v2);
      REQUIRE(cudaMalloc(&dy, N*sizeof(T)) == cudaSuccess);
    }).name("allocate_y");
    
    // axpy
    auto cudaflow = taskflow.emplace([&](F& cf) {
      auto h2d_x = cf.copy(dx, hx.data(), N).name("h2d_x");
      auto h2d_y = cf.copy(dy, hy.data(), N).name("h2d_y");
      auto d2h_x = cf.copy(hx.data(), dx, N).name("d2h_x");
      auto d2h_y = cf.copy(hy.data(), dy, N).name("d2h_y");
      //auto kernel = cf.add(dx, N, dx, dy);
      auto kernel = cf.transform(dx, dx+N, dy, 
        [] __device__ (T x) { return x + 2;  }
      );
      kernel.succeed(h2d_x, h2d_y)
            .precede(d2h_x, d2h_y);
    }).name("saxpy");

    cudaflow.succeed(allocate_x, allocate_y);

    // Add a verification task
    auto verifier = taskflow.emplace([&](){
      for (size_t i = 0; i < N; i++) {
        REQUIRE(std::fabs(hx[i] - v1) < eps);
        REQUIRE(std::fabs(hy[i] - (hx[i] + 2)) < eps);
      }
    }).succeed(cudaflow).name("verify");

    // free memory
    auto deallocate_x = taskflow.emplace([&](){
      REQUIRE(cudaFree(dx) == cudaSuccess);
    }).name("deallocate_x");
    
    auto deallocate_y = taskflow.emplace([&](){
      REQUIRE(cudaFree(dy) == cudaSuccess);
    }).name("deallocate_y");

    verifier.precede(deallocate_x, deallocate_y);

    executor.run(taskflow).wait();

    // standalone tramsform
    tf::cudaDefaultExecutionPolicy p;

    auto input  = tf::cuda_malloc_shared<T>(N);
    auto output = tf::cuda_malloc_shared<T>(N);
    for(size_t n=0; n<N; n++) {
      input[n] = 1;
    }

    tf::cuda_transform(p, input, input + N, output, 
      [] __device__ (T i) { return i+2; }
    );
    cudaStreamSynchronize(0);

    for(size_t n=0; n<N; n++) {
      REQUIRE(output[n] == 3);
    }
  }
}

TEST_CASE("add2.int" * doctest::timeout(300)) {
  add2<int, tf::cudaFlow>();
}

TEST_CASE("add2.float" * doctest::timeout(300)) {
  add2<float, tf::cudaFlow>();
}

TEST_CASE("add2.double" * doctest::timeout(300)) {
  add2<double, tf::cudaFlow>();
}

TEST_CASE("capture_add2.int" * doctest::timeout(300)) {
  add2<int, tf::cudaFlowCapturer>();
}

TEST_CASE("capture_add2.float" * doctest::timeout(300)) {
  add2<float, tf::cudaFlowCapturer>();
}

TEST_CASE("capture_add2.double" * doctest::timeout(300)) {
  add2<double, tf::cudaFlowCapturer>();
}

// ----------------------------------------------------------------------------
// for_each
// ----------------------------------------------------------------------------

template <typename T, typename F>
void for_each() {
    
  tf::Taskflow taskflow;
  tf::Executor executor;

  for(int n=1; n<=1234567; n = n*2 + 1) {

    taskflow.clear();
    
    T* cpu = nullptr;
    T* gpu = nullptr;

    auto cputask = taskflow.emplace([&](){
      cpu = static_cast<T*>(std::calloc(n, sizeof(T)));
      REQUIRE(cudaMalloc(&gpu, n*sizeof(T)) == cudaSuccess);
    });

    tf::Task gputask;
    
    gputask = taskflow.emplace([&](F& cf) {
      auto d2h = cf.copy(cpu, gpu, n);
      auto h2d = cf.copy(gpu, cpu, n);
      auto kernel = cf.for_each(
        gpu, gpu+n, [] __device__ (T& val) { val = 65536; }
      );
      h2d.precede(kernel);
      d2h.succeed(kernel);
    });

    cputask.precede(gputask);
    
    executor.run(taskflow).wait();

    for(int i=0; i<n; i++) {
      REQUIRE(std::fabs(cpu[i] - (T)65536) < eps);
    }

    std::free(cpu);
    REQUIRE(cudaFree(gpu) == cudaSuccess);

    // standard algorithm: for_each
    auto g_data = tf::cuda_malloc_shared<T>(n);

    for(int i=0; i<n; i++) {
      g_data[i] = 0;
    }

    tf::cuda_for_each(tf::cudaDefaultExecutionPolicy{},
      g_data, g_data + n, [] __device__ (T& val) { val = 12222; }
    );

    cudaStreamSynchronize(0);
    
    for(int i=0; i<n; i++) {
      REQUIRE(std::fabs(g_data[i] - (T)12222) < eps);
    }

    tf::cuda_free(g_data);
  }
}

TEST_CASE("cudaflow.for_each.int" * doctest::timeout(300)) {
  for_each<int, tf::cudaFlow>();
}

TEST_CASE("cudaflow.for_each.float" * doctest::timeout(300)) {
  for_each<float, tf::cudaFlow>();
}

TEST_CASE("cudaflow.for_each.double" * doctest::timeout(300)) {
  for_each<double, tf::cudaFlow>();
}

TEST_CASE("capture.for_each.int" * doctest::timeout(300)) {
  for_each<int, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.for_each.float" * doctest::timeout(300)) {
  for_each<float, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.for_each.double" * doctest::timeout(300)) {
  for_each<double, tf::cudaFlowCapturer>();
}

// --------------------------------------------------------
// Testcase: for_each_index
// --------------------------------------------------------

template <typename T, typename F>
void for_each_index() {

  for(int n=10; n<=1234567; n = n*2 + 1) {

    tf::Taskflow taskflow;
    tf::Executor executor;
    
    T* cpu = nullptr;
    T* gpu = nullptr;

    auto cputask = taskflow.emplace([&](){
      cpu = static_cast<T*>(std::calloc(n, sizeof(T)));
      REQUIRE(cudaMalloc(&gpu, n*sizeof(T)) == cudaSuccess);
    });

    auto gputask = taskflow.emplace([&](F& cf) {
      auto d2h = cf.copy(cpu, gpu, n);
      auto h2d = cf.copy(gpu, cpu, n);
      //auto kernel = cf.for_each_index(gpu, n, [] __device__ (T& value){ value = 17; });
      auto kernel1 = cf.for_each_index(
        0, n, 2, 
        [gpu] __device__ (int i) { gpu[i] = 17; }
      );
      auto kernel2 = cf.for_each_index(
        1, n, 2, 
        [=] __device__ (int i) { gpu[i] = -17; }
      );
      h2d.precede(kernel1, kernel2);
      d2h.succeed(kernel1, kernel2);
    });

    cputask.precede(gputask);
    
    executor.run(taskflow).wait();

    for(int i=0; i<n; i++) {
      if(i % 2 == 0) {
        REQUIRE(std::fabs(cpu[i] - (T)17) < eps);
      }
      else {
        REQUIRE(std::fabs(cpu[i] - (T)(-17)) < eps);
      }
    }

    std::free(cpu);
    REQUIRE(cudaFree(gpu) == cudaSuccess);
  }
}

TEST_CASE("cudaflow.for_each_index.int" * doctest::timeout(300)) {
  for_each_index<int, tf::cudaFlow>();
}

TEST_CASE("cudaflow.for_each_index.float" * doctest::timeout(300)) {
  for_each_index<float, tf::cudaFlow>();
}

TEST_CASE("cudaflow.for_each_index.double" * doctest::timeout(300)) {
  for_each_index<double, tf::cudaFlow>();
}

TEST_CASE("capture.for_each_index.int" * doctest::timeout(300)) {
  for_each_index<int, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.for_each_index.float" * doctest::timeout(300)) {
  for_each_index<float, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.for_each_index.double" * doctest::timeout(300)) {
  for_each_index<double, tf::cudaFlowCapturer>();
}

// ----------------------------------------------------------------------------
// transform
// ----------------------------------------------------------------------------

template <typename F>
void transform() {
  
  F cudaflow;
    
  for(unsigned n=1; n<=1234567; n = n*2 + 1) {

    cudaflow.clear();

    auto src1 = tf::cuda_malloc_shared<int>(n);
    auto src2 = tf::cuda_malloc_shared<int>(n);
    auto dest = tf::cuda_malloc_shared<int>(n);

    for(unsigned i=0; i<n; i++) {
      src1[i] = 10;
      src2[i] = 90;
      dest[i] = 0;
    }

    cudaflow.transform(src1, src1+n, src2, dest,
      []__device__(int s1, int s2) { return s1 + s2; } 
    );

    cudaflow.offload();

    for(unsigned i=0; i<n; i++){
      REQUIRE(dest[i] == src1[i] + src2[i]);
    }
  }
}

TEST_CASE("cudaflow.transform" * doctest::timeout(300)) {
  transform<tf::cudaFlow>();
}

TEST_CASE("capture.transform" * doctest::timeout(300) ) {
  transform<tf::cudaFlowCapturer>();
}

// ----------------------------------------------------------------------------
// reduce
// ----------------------------------------------------------------------------

template <typename T, typename F>
void reduce() {

  for(int n=1; n<=1234567; n = n*2 + 1) {

    tf::Taskflow taskflow;
    tf::Executor executor;

    T sum = 0;

    std::vector<T> cpu(n);
    for(auto& i : cpu) {
      i = ::rand()%100-50;
      sum += i;
    }

    T sol;
    
    T* gpu = nullptr;
    T* res = nullptr;

    auto cputask = taskflow.emplace([&](){
      REQUIRE(cudaMalloc(&gpu, n*sizeof(T)) == cudaSuccess);
      REQUIRE(cudaMalloc(&res, 1*sizeof(T)) == cudaSuccess);
    });

    tf::Task gputask;
    
    gputask = taskflow.emplace([&](F& cf) {
      auto d2h = cf.copy(&sol, res, 1);
      auto h2d = cf.copy(gpu, cpu.data(), n);
      auto set = cf.single_task([res] __device__ () mutable {
        *res = 1000;
      });
      auto kernel = cf.reduce(
        gpu, gpu+n, res, [] __device__ (T a, T b) mutable { 
          return a + b;
        }
      );
      kernel.succeed(h2d, set);
      d2h.succeed(kernel);
    });

    cputask.precede(gputask);
    
    executor.run(taskflow).wait();

    REQUIRE(std::fabs(sum-sol+1000) < 0.0001);

    REQUIRE(cudaFree(gpu) == cudaSuccess);
    REQUIRE(cudaFree(res) == cudaSuccess);
  }
}

TEST_CASE("cudaflow.reduce.int" * doctest::timeout(300)) {
  reduce<int, tf::cudaFlow>();
}

TEST_CASE("cudaflow.reduce.float" * doctest::timeout(300)) {
  reduce<float, tf::cudaFlow>();
}

TEST_CASE("cudaflow.reduce.double" * doctest::timeout(300)) {
  reduce<double, tf::cudaFlow>();
}

TEST_CASE("capture.reduce.int" * doctest::timeout(300)) {
  reduce<int, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.reduce.float" * doctest::timeout(300)) {
  reduce<float, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.reduce.double" * doctest::timeout(300)) {
  reduce<double, tf::cudaFlow>();
}

// ----------------------------------------------------------------------------
// uninitialized_reduce
// ----------------------------------------------------------------------------

template <typename T, typename F>
void uninitialized_reduce() {

  for(int n=1; n<=1234567; n = n*2 + 1) {

    tf::Taskflow taskflow;
    tf::Executor executor;

    T sum = 0;

    std::vector<T> cpu(n);
    for(auto& i : cpu) {
      i = ::rand()%100-50;
      sum += i;
    }

    T sol;
    
    T* gpu = nullptr;
    T* res = nullptr;

    auto cputask = taskflow.emplace([&](){
      REQUIRE(cudaMalloc(&gpu, n*sizeof(T)) == cudaSuccess);
      REQUIRE(cudaMalloc(&res, 1*sizeof(T)) == cudaSuccess);
    });

    tf::Task gputask;
    
    gputask = taskflow.emplace([&](F& cf) {
      auto d2h = cf.copy(&sol, res, 1);
      auto h2d = cf.copy(gpu, cpu.data(), n);
      auto set = cf.single_task([res] __device__ () mutable {
        *res = 1000;
      });
      auto kernel = cf.uninitialized_reduce(
        gpu, gpu+n, res, [] __device__ (T a, T b) { 
          return a + b;
        }
      );
      kernel.succeed(h2d, set);
      d2h.succeed(kernel);
    });

    cputask.precede(gputask);
    
    executor.run(taskflow).wait();

    REQUIRE(std::fabs(sum-sol) < 0.0001);

    REQUIRE(cudaFree(gpu) == cudaSuccess);
    REQUIRE(cudaFree(res) == cudaSuccess);
  }
}

TEST_CASE("cudaflow.uninitialized_reduce.int" * doctest::timeout(300)) {
  uninitialized_reduce<int, tf::cudaFlow>();
}

TEST_CASE("cudaflow.uninitialized_reduce.float" * doctest::timeout(300)) {
  uninitialized_reduce<float, tf::cudaFlow>();
}

TEST_CASE("cudaflow.uninitialized_reduce.double" * doctest::timeout(300)) {
  uninitialized_reduce<double, tf::cudaFlow>();
}

TEST_CASE("capture.uninitialized_reduce.int" * doctest::timeout(300)) {
  uninitialized_reduce<int, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.uninitialized_reduce.float" * doctest::timeout(300)) {
  uninitialized_reduce<float, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.uninitialized_reduce.double" * doctest::timeout(300)) {
  uninitialized_reduce<double, tf::cudaFlow>();
}

// ----------------------------------------------------------------------------
// transform_reduce
// ----------------------------------------------------------------------------

template <typename T, typename F>
void transform_reduce() {
    
  tf::Executor executor;

  for(int n=1; n<=1234567; n = n*2 + 1) {

    tf::Taskflow taskflow;

    T sum = 0;

    std::vector<T> cpu(n);
    for(auto& i : cpu) {
      i = ::rand()%100-50;
      sum += i;
    }

    T sol;
    
    T* gpu = nullptr;
    T* res = nullptr;

    auto cputask = taskflow.emplace([&](){
      REQUIRE(cudaMalloc(&gpu, n*sizeof(T)) == cudaSuccess);
      REQUIRE(cudaMalloc(&res, 1*sizeof(T)) == cudaSuccess);
    });

    tf::Task gputask;
    
    gputask = taskflow.emplace([&](F& cf) {
      auto d2h = cf.copy(&sol, res, 1);
      auto h2d = cf.copy(gpu, cpu.data(), n);
      auto set = cf.single_task([res] __device__ () mutable {
        *res = 1000;
      });
      auto kernel = cf.transform_reduce(
        gpu, gpu+n, res, 
        [] __device__ (T a, T b) { return a + b; },
        [] __device__ (T a) { return a + 1; }
      );
      kernel.succeed(h2d, set);
      d2h.succeed(kernel);
    });

    cputask.precede(gputask);
    
    executor.run(taskflow).wait();

    REQUIRE(std::fabs(sum+n+1000-sol) < 0.0001);

    REQUIRE(cudaFree(gpu) == cudaSuccess);
    REQUIRE(cudaFree(res) == cudaSuccess);
  }
}

TEST_CASE("cudaflow.transform_reduce.int" * doctest::timeout(300)) {
  transform_reduce<int, tf::cudaFlow>();
}

TEST_CASE("cudaflow.transform_reduce.float" * doctest::timeout(300)) {
  transform_reduce<float, tf::cudaFlow>();
}

TEST_CASE("cudaflow.transform_reduce.double" * doctest::timeout(300)) {
  transform_reduce<double, tf::cudaFlow>();
}

TEST_CASE("capture.transform_reduce.int" * doctest::timeout(300)) {
  transform_reduce<int, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.transform_reduce.float" * doctest::timeout(300)) {
  transform_reduce<float, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.transform_reduce.double" * doctest::timeout(300)) {
  transform_reduce<double, tf::cudaFlowCapturer>();
}

// ----------------------------------------------------------------------------
// transform_uninitialized_reduce
// ----------------------------------------------------------------------------

template <typename T, typename F>
void transform_uninitialized_reduce() {
    
  tf::Executor executor;

  for(int n=1; n<=1234567; n = n*2 + 1) {

    tf::Taskflow taskflow;

    T sum = 0;

    std::vector<T> cpu(n);
    for(auto& i : cpu) {
      i = ::rand()%100-50;
      sum += i;
    }

    T sol;
    
    T* gpu = nullptr;
    T* res = nullptr;

    auto cputask = taskflow.emplace([&](){
      REQUIRE(cudaMalloc(&gpu, n*sizeof(T)) == cudaSuccess);
      REQUIRE(cudaMalloc(&res, 1*sizeof(T)) == cudaSuccess);
    });

    tf::Task gputask;
    
    gputask = taskflow.emplace([&](F& cf) {
      auto d2h = cf.copy(&sol, res, 1);
      auto h2d = cf.copy(gpu, cpu.data(), n);
      auto set = cf.single_task([res] __device__ () mutable {
        *res = 1000;
      });
      auto kernel = cf.transform_uninitialized_reduce(
        gpu, gpu+n, res, 
        [] __device__ (T a, T b) { return a + b; },
        [] __device__ (T a) { return a + 1; }
      );
      kernel.succeed(h2d, set);
      d2h.succeed(kernel);
    });

    cputask.precede(gputask);
    
    executor.run(taskflow).wait();

    REQUIRE(std::fabs(sum+n-sol) < 0.0001);

    REQUIRE(cudaFree(gpu) == cudaSuccess);
    REQUIRE(cudaFree(res) == cudaSuccess);
  }
}

TEST_CASE("cudaflow.transform_uninitialized_reduce.int" * doctest::timeout(300)) {
  transform_uninitialized_reduce<int, tf::cudaFlow>();
}

TEST_CASE("cudaflow.transform_uninitialized_reduce.float" * doctest::timeout(300)) {
  transform_uninitialized_reduce<float, tf::cudaFlow>();
}

TEST_CASE("cudaflow.transform_uninitialized_reduce.double" * doctest::timeout(300)) {
  transform_uninitialized_reduce<double, tf::cudaFlow>();
}

TEST_CASE("capture.transform_uninitialized_reduce.int" * doctest::timeout(300)) {
  transform_uninitialized_reduce<int, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.transform_uninitialized_reduce.float" * doctest::timeout(300)) {
  transform_uninitialized_reduce<float, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.transform_uninitialized_reduce.double" * doctest::timeout(300)) {
  transform_uninitialized_reduce<double, tf::cudaFlowCapturer>();
}

// ----------------------------------------------------------------------------
// scan
// ----------------------------------------------------------------------------

template <typename T, typename F>
void scan() {
    
  tf::Executor executor;
  tf::Taskflow taskflow;

  for(int N=1; N<=1234567; N = N*2 + 1) {

    taskflow.clear();
  
    auto data1 = tf::cuda_malloc_shared<T>(N);
    auto data2 = tf::cuda_malloc_shared<T>(N);
    auto scan1 = tf::cuda_malloc_shared<T>(N);
    auto scan2 = tf::cuda_malloc_shared<T>(N);

    // initialize the data
    for(int i=0; i<N; i++) {
      data1[i] = T(i);
      data2[i] = T(i);
      scan1[i] = 0;
      scan2[i] = 0;
    }
    
    // perform reduction
    taskflow.emplace([&](F& cudaflow){
      // inclusive scan
      cudaflow.inclusive_scan(
        data1, data1+N, scan1, [] __device__ (T a, T b){ return a+b; }
      );
      // exclusive scan
      cudaflow.exclusive_scan(
        data2, data2+N, scan2, [] __device__ (T a, T b){ return a+b; }
      );
    });

    executor.run(taskflow).wait();
    
    // inspect 
    for(int i=1; i<N; i++) {
      REQUIRE(scan1[i] == (scan1[i-1]+data1[i]));
      REQUIRE(scan2[i] == (scan2[i-1]+data2[i-1]));
    }

    // test standalone algorithms
    
    // initialize the data
    for(int i=0; i<N; i++) {
      data1[i] = T(i);
      data2[i] = T(i);
      scan1[i] = 0;
      scan2[i] = 0;
    }

    // allocate temporary buffer
    tf::cudaScopedDeviceMemory<std::byte> temp(
      tf::cuda_scan_buffer_size<tf::cudaDefaultExecutionPolicy, T>(N)
    );
      
    tf::cuda_inclusive_scan(tf::cudaDefaultExecutionPolicy{}, 
      data1, data1+N, scan1, tf::cuda_plus<T>{}, temp.data()
    );
    cudaStreamSynchronize(0);

    tf::cuda_exclusive_scan(tf::cudaDefaultExecutionPolicy{}, 
      data2, data2+N, scan2, tf::cuda_plus<T>{}, temp.data()
    );
    cudaStreamSynchronize(0);
    
    // inspect 
    for(int i=1; i<N; i++) {
      REQUIRE(scan1[i] == (scan1[i-1]+data1[i]));
      REQUIRE(scan2[i] == (scan2[i-1]+data2[i-1]));
    }

    REQUIRE(cudaFree(data1) == cudaSuccess);
    REQUIRE(cudaFree(data2) == cudaSuccess);
    REQUIRE(cudaFree(scan1) == cudaSuccess);
    REQUIRE(cudaFree(scan2) == cudaSuccess);
  }
}

TEST_CASE("cudaflow.scan.int" * doctest::timeout(300)) {
  scan<int, tf::cudaFlow>();
}

TEST_CASE("cudaflow.scan.size_t" * doctest::timeout(300)) {
  scan<size_t, tf::cudaFlow>();
}

TEST_CASE("capture.scan.int" * doctest::timeout(300)) {
  scan<int, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.scan.size_t" * doctest::timeout(300)) {
  scan<size_t, tf::cudaFlowCapturer>();
}

// ----------------------------------------------------------------------------
// transofrm scan
// ----------------------------------------------------------------------------

template <typename T, typename F>
void transform_scan() {
    
  tf::Executor executor;
  tf::Taskflow taskflow;

  for(int N=1; N<=1234567; N = N*2 + 1) {

    taskflow.clear();
  
    auto data1 = tf::cuda_malloc_shared<T>(N);
    auto data2 = tf::cuda_malloc_shared<T>(N);
    auto scan1 = tf::cuda_malloc_shared<T>(N);
    auto scan2 = tf::cuda_malloc_shared<T>(N);

    // initialize the data
    for(int i=0; i<N; i++) {
      data1[i] = T(i);
      data2[i] = T(i);
      scan1[i] = 0;
      scan2[i] = 0;
    }
    
    // perform reduction
    taskflow.emplace([&](F& cudaflow){
      // inclusive scan
      cudaflow.transform_inclusive_scan(
        data1, data1+N, scan1, 
        [] __device__ (T a, T b){ return a+b; },
        [] __device__ (T a) { return a*10; }
      );
      // exclusive scan
      cudaflow.transform_exclusive_scan(
        data2, data2+N, scan2, 
        [] __device__ (T a, T b){ return a+b; },
        [] __device__ (T a) { return a*10; }
      );
    });

    executor.run(taskflow).wait();
    
    // standalone algorithms
    
    // initialize the data
    for(int i=0; i<N; i++) {
      data1[i] = T(i);
      data2[i] = T(i);
      scan1[i] = 0;
      scan2[i] = 0;
    }
    
    // allocate temporary buffer
    tf::cudaScopedDeviceMemory<std::byte> temp(
      tf::cuda_scan_buffer_size<tf::cudaDefaultExecutionPolicy, T>(N)
    );
      
    tf::cuda_transform_inclusive_scan(tf::cudaDefaultExecutionPolicy{},
      data1, data1+N, scan1, 
      [] __device__ (T a, T b){ return a+b; },
      [] __device__ (T a) { return a*10; },
      temp.data()
    );
    cudaStreamSynchronize(0);
      
    tf::cuda_transform_exclusive_scan(tf::cudaDefaultExecutionPolicy{},
      data2, data2+N, scan2, 
      [] __device__ (T a, T b){ return a+b; },
      [] __device__ (T a) { return a*10; },
      temp.data()
    );
    cudaStreamSynchronize(0);
    
    // inspect 
    for(int i=1; i<N; i++) {
      REQUIRE(scan1[i] == (scan1[i-1]+data1[i]*10));
      REQUIRE(scan2[i] == (scan2[i-1]+data2[i-1]*10));
    }

    REQUIRE(cudaFree(data1) == cudaSuccess);
    REQUIRE(cudaFree(data2) == cudaSuccess);
    REQUIRE(cudaFree(scan1) == cudaSuccess);
    REQUIRE(cudaFree(scan2) == cudaSuccess);
  }
}

TEST_CASE("cudaflow.scan.int" * doctest::timeout(300)) {
  transform_scan<int, tf::cudaFlow>();
}

TEST_CASE("cudaflow.scan.size_t" * doctest::timeout(300)) {
  transform_scan<size_t, tf::cudaFlow>();
}

TEST_CASE("capture.transform_scan.int" * doctest::timeout(300)) {
  transform_scan<int, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.transform_scan.size_t" * doctest::timeout(300)) {
  transform_scan<size_t, tf::cudaFlowCapturer>();
}

// ----------------------------------------------------------------------------
// merge
// ----------------------------------------------------------------------------

template <typename T, typename F>
void merge_keys() {
    
  tf::Executor executor;
  tf::Taskflow taskflow;

  for(int N=0; N<=1234567; N = N*2 + 1) {

    taskflow.clear();

    auto a = tf::cuda_malloc_shared<T>(N);
    auto b = tf::cuda_malloc_shared<T>(N);
    auto c = tf::cuda_malloc_shared<T>(2*N);

    auto p = tf::cudaDefaultExecutionPolicy{};

    // ----------------- standalone algorithms

    // initialize the data
    for(int i=0; i<N; i++) {
      a[i] = T(rand()%100);
      b[i] = T(rand()%100);
    }

    std::sort(a, a+N);
    std::sort(b, b+N);
    
    auto bufsz = tf::cuda_merge_buffer_size<decltype(p)>(N, N);
    tf::cudaScopedDeviceMemory<std::byte> buf(bufsz);

    tf::cuda_merge(p, a, a+N, b, b+N, c, tf::cuda_less<T>{}, buf.data());
    p.synchronize();

    REQUIRE(std::is_sorted(c, c+2*N));
    
    // ----------------- cudaFlow capturer
    for(int i=0; i<N*2; i++) {
      c[i] = rand();      
    }
    
    taskflow.emplace([&](F& cudaflow){
      cudaflow.merge(a, a+N, b, b+N, c, tf::cuda_less<T>{});
    });

    executor.run(taskflow).wait();
    
    REQUIRE(std::is_sorted(c, c+2*N));
    
    REQUIRE(cudaFree(a) == cudaSuccess);
    REQUIRE(cudaFree(b) == cudaSuccess);
    REQUIRE(cudaFree(c) == cudaSuccess);
  }
}

TEST_CASE("cudaflow.merge_keys.int" * doctest::timeout(300)) {
  merge_keys<int, tf::cudaFlow>();
}

TEST_CASE("cudaflow.merge_keys.float" * doctest::timeout(300)) {
  merge_keys<float, tf::cudaFlow>();
}

TEST_CASE("cudaflow.merge_keys.int" * doctest::timeout(300)) {
  merge_keys<int, tf::cudaFlow>();
}

TEST_CASE("capture.merge_keys.float" * doctest::timeout(300)) {
  merge_keys<float, tf::cudaFlowCapturer>();
}

// ----------------------------------------------------------------------------
// sort
// ----------------------------------------------------------------------------

template <typename T, typename F>
void sort_keys() {
    
  tf::Executor executor;
  tf::Taskflow taskflow;

  for(int N=0; N<=1234567; N = N*2 + 1) {

    taskflow.clear();

    auto a = tf::cuda_malloc_shared<T>(N);
    auto p = tf::cudaDefaultExecutionPolicy{};

    // ----------------- standalone asynchronous algorithms

    // initialize the data
    for(int i=0; i<N; i++) {
      a[i] = T(rand()%1000);
    }

    auto bufsz = tf::cuda_sort_buffer_size<decltype(p), T>(N);
    tf::cudaScopedDeviceMemory<std::byte> buf(bufsz);
    tf::cuda_sort(p, a, a+N, tf::cuda_less<T>{}, buf.data());
    p.synchronize();
    REQUIRE(std::is_sorted(a, a+N));

    // ----------------- cudaflow capturer
    for(int i=0; i<N; i++) {
      a[i] = T(rand()%1000);
    }
    
    taskflow.emplace([&](F& cudaflow){
      cudaflow.sort(a, a+N, tf::cuda_less<T>{});
    });

    executor.run(taskflow).wait();

    REQUIRE(std::is_sorted(a, a+N));
    
    REQUIRE(cudaFree(a) == cudaSuccess);
  }
}

TEST_CASE("cudaflow.sort_keys.int" * doctest::timeout(300)) {
  sort_keys<int, tf::cudaFlow>();
}

TEST_CASE("cudaflow.sort_keys.float" * doctest::timeout(300)) {
  sort_keys<float, tf::cudaFlow>();
}

TEST_CASE("capture.sort_keys.int" * doctest::timeout(300)) {
  sort_keys<int, tf::cudaFlowCapturer>();
}

TEST_CASE("capture.sort_keys.float" * doctest::timeout(300)) {
  sort_keys<float, tf::cudaFlowCapturer>();
}

/*// --------------------------------------------------------------------------
// row-major transpose
// ----------------------------------------------------------------------------

// Disable for now - better to use cublasFlowCapturer

template <typename T>
__global__
void verify(const T* din_mat, const T* dout_mat, bool* check, size_t rows, size_t cols) {
  
  size_t tid = blockDim.x * blockIdx.x + threadIdx.x;
  size_t size = rows * cols;
  for(; tid < size; tid += gridDim.x * blockDim.x) {
    if(din_mat[tid] != dout_mat[tid / cols + (tid % cols) * rows]) {
      *check = false;
      return;
    }
  }
}

template <typename T>
void transpose() {
  tf::Executor executor;

  for(size_t rows = 1; rows <= 7999; rows*=2+3) {
    for(size_t cols = 1; cols <= 8021; cols*=3+5) {

      tf::Taskflow taskflow;
      std::vector<T> hinput_mat(rows * cols);

      std::generate_n(hinput_mat.begin(), rows * cols, [](){ return ::rand(); });

      T* dinput_mat {nullptr};
      T* doutput_mat {nullptr};
      bool* check {nullptr};
      
       //allocate
      auto allocate = taskflow.emplace([&]() {
        REQUIRE(cudaMalloc(&dinput_mat, (rows * cols) * sizeof(T)) == cudaSuccess);
        REQUIRE(cudaMalloc(&doutput_mat, (rows * cols) * sizeof(T)) == cudaSuccess);
        REQUIRE(cudaMallocManaged(&check, sizeof(bool)) == cudaSuccess);
        *check = true;
      }).name("allocate");

       //transpose
      auto cudaflow = taskflow.emplace([&](tf::cudaFlow& cf) {
        auto h2d_input_t = cf.copy(dinput_mat, hinput_mat.data(), rows * cols).name("h2d");

        auto kernel_t = tf::cudaBLAF(cf).transpose(
          dinput_mat,
          doutput_mat,
          rows,
          cols
        );

        auto verify_t = cf.kernel(
          32,
          512,
          0,
          verify<T>,
          dinput_mat,
          doutput_mat,
          check,
          rows,
          cols
        );

        h2d_input_t.precede(kernel_t);
        kernel_t.precede(verify_t);
      }).name("transpose");


       //free memory
      auto deallocate = taskflow.emplace([&](){
        REQUIRE(cudaFree(dinput_mat) == cudaSuccess);
        REQUIRE(cudaFree(doutput_mat) == cudaSuccess);
      }).name("deallocate");
      

      allocate.precede(cudaflow);
      cudaflow.precede(deallocate);

      executor.run(taskflow).wait();
      REQUIRE(*check);
    }
  }
}

TEST_CASE("transpose.int" * doctest::timeout(300) ) {
  transpose<int>();
}

TEST_CASE("transpose.float" * doctest::timeout(300) ) {
  transpose<float>();
}


TEST_CASE("transpose.double" * doctest::timeout(300) ) {
  transpose<double>();
}

// ----------------------------------------------------------------------------
// row-major matrix multiplication
// ----------------------------------------------------------------------------

template <typename T>
void matmul() {
  tf::Taskflow taskflow;
  tf::Executor executor;
  
  std::vector<T> a, b, c;

  for(int m=1; m<=1992; m=2*m+1) {
    for(int k=1; k<=1012; k=2*k+3) {
      for(int n=1; n<=1998; n=2*n+8) {

        taskflow.clear();

        T* ha {nullptr};
        T* hb {nullptr};
        T* hc {nullptr};
        T* da {nullptr};
        T* db {nullptr};
        T* dc {nullptr};
      
        T val_a = ::rand()%5-1;
        T val_b = ::rand()%7-3;

        auto hosta = taskflow.emplace([&](){ 
          a.resize(m*k);
          std::fill_n(a.begin(), m*k, val_a);
          ha = a.data();
          REQUIRE(cudaMalloc(&da, m*k*sizeof(T)) == cudaSuccess);
        }).name("ha");

        auto hostb = taskflow.emplace([&](){ 
          b.resize(k*n);
          std::fill_n(b.begin(), k*n, val_b);
          hb = b.data();
          REQUIRE(cudaMalloc(&db, k*n*sizeof(T)) == cudaSuccess);
        }).name("hb");

        auto hostc = taskflow.emplace([&](){
          c.resize(m*n);
          hc = c.data();
          REQUIRE(cudaMalloc(&dc, m*n*sizeof(T)) == cudaSuccess);
        }).name("hc");

        auto cuda = taskflow.emplace([&](tf::cudaFlow& cf){
          auto pa = cf.copy(da, ha, m*k);
          auto pb = cf.copy(db, hb, k*n);

          auto op = tf::cudaBLAF(cf).matmul(
            da, db, dc, m, k, n 
          ).name("op");

          auto cc = cf.copy(hc, dc, m*n).name("cc");

          op.precede(cc).succeed(pa, pb);
        });

        cuda.succeed(hosta, hostb, hostc);

        executor.run(taskflow).wait();

        int ans = val_a*val_b*k;
        for(const auto& x : c) {
          REQUIRE((int)x == ans);
        }

        REQUIRE(cudaFree(da) == cudaSuccess);
        REQUIRE(cudaFree(db) == cudaSuccess);
        REQUIRE(cudaFree(dc) == cudaSuccess);
      }
    }
  }
}

TEST_CASE("matmul.int" * doctest::timeout(300) ) {
  matmul<int>();
}

TEST_CASE("matmul.float" * doctest::timeout(300) ) {
  matmul<float>();
}

TEST_CASE("matmul.double" * doctest::timeout(300) ) {
  matmul<double>();
}*/

