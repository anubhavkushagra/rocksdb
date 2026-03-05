# Full 3-Node Distributed KV Server Setup Guide

This covers the complete setup from scratch for all 3 laptops: App Server (AMD Ryzen), Storage Server (Intel i5), and Client (Intel i3).

> **IMPORTANT:** Laptop 2 (App Server) and Laptop 3 (Storage Server) must be on the **same LAN via Ethernet** for maximum performance. The Client (i3) can be on Wi-Fi.

## 1. Architecture Overview (1 Server, 3 Client Nodes)

You have 3 laptops with the following roles and capacities:
*   **Machine 1 (Main/Server):** 12th Gen Intel Core i5-12500H (16 Threads, 16GB RAM)
    *   **Role:** Runs the gRPC Server + Load Client A
    *   **Capacity:** Extreme. After the server takes its toll (~8 threads), the client can easily push 8 heavy threads.
*   **Machine 2:** AMD Ryzen 5 4600H (12 Threads, 8GB RAM)
    *   **Role:** Load Client B
    *   **Capacity:** High. Allocate 12 parallel pushing threads.
*   **Machine 3:** 11th Gen Intel Core i3-1115G4 (4 Threads, 4GB RAM)
    *   **Role:** Load Client C
    *   **Capacity:** Low. Allocate 4 parallel pushing threads to avoid freezing the system.

*(Laptop 2 and 3 must be connected via LAN ethernet cable. Client i3 can be anywhere.)*

---

---

## 2. Client Code (`distributed_load_client.cpp`)

This is the load generator binary for **all 3 laptops**. Create a file called `kv_load_client/distributed_load_client.cpp` with this code:

**Key improvements over the original client:**
1. **`--target <IP:PORT>`** argument so different laptops can point to the same App Server.
2. **TLS Target Name Override** — because self-signed certificates are tied to `localhost`, we override the SSL hostname check to work over LAN IPs while keeping full encryption.
3. **`--node-id`** — Each laptop gets a unique node ID that partitions the key space so laptops don't accidentally fight over the exact same keys, giving clean benchmark numbers.

```cpp
#include <iostream>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <csignal>
#include <grpcpp/grpcpp.h>
#include "kv.grpc.pb.h"

// --- SECURITY HELPER: Read Certificate Files ---
std::string ReadFile(const std::string& filename) {
    std::ifstream ifs(filename);
    if (!ifs.is_open()) {
        std::cerr << "CRITICAL ERROR: Failed to open " << filename << ". Are you running from the correct folder?" << std::endl;
        exit(1);
    }
    return std::string((std::istreambuf_iterator<char>(ifs)),
                       std::istreambuf_iterator<char>());
}

struct AsyncCall {
    kv::BatchResponse reply;
    grpc::ClientContext ctx;
    grpc::Status status;
    std::chrono::steady_clock::time_point start;
    std::unique_ptr<grpc::ClientAsyncResponseReader<kv::BatchResponse>> reader;
};

class Benchmarker {
public:
    std::atomic<long> puts{0}, gets{0}, deletes{0}, errors{0};
    std::atomic<long> latency_sum_us{0};
    std::atomic<bool> running{true};

    void print_final_stats(int threads, int batch_sz, int duration, const std::string& target) {

        long p = puts.load(), g = gets.load(), d = deletes.load();
        long total = p + g + d;
        double actual_duration = duration;
        double throughput = (actual_duration > 0) ? ((double)total / actual_duration) : 0;

        long total_rpcs = total / batch_sz;
        double avg_rpc_lat_ms = (total_rpcs > 0) ? ((double)latency_sum_us / 1000.0) / total_rpcs : 0;
        double avg_op_lat_ms = (total > 0) ? ((double)latency_sum_us / 1000.0) / total : 0;

        std::cout << "\n\033[1;36m" << "================== FINAL BENCHMARK REPORT ==================" << "\033[0m\n";
        std::cout << std::left << std::setw(30) << "METRIC" << "VALUE" << "\n";
        std::cout << "------------------------------------------------------------\n";
        std::cout << std::setw(30) << "Target Server" << target << "\n";
        std::cout << std::setw(30) << "Parallel Threads" << threads << "\n";
        std::cout << std::setw(30) << "Batch Size (Keys/RPC)" << batch_sz << "\n";
        std::cout << std::setw(30) << "Test Duration" << duration << "s\n";
        std::cout << "------------------------------------------------------------\n";
        std::cout << std::setw(30) << "CREATE (PUT) Ops" << p << "\n";
        std::cout << std::setw(30) << "READ (GET) Ops" << g << "\n";
        std::cout << std::setw(30) << "DELETE Ops" << d << "\n";
        std::cout << "------------------------------------------------------------\n";
        std::cout << std::setw(30) << "TOTAL OPERATIONS" << total << "\n";
        std::cout << std::setw(30) << "FAILED RPCs" << "\033[1;31m" << errors.load() << "\033[0m\n";
        std::cout << "------------------------------------------------------------\n";
        std::cout << " THROUGHPUT:          \033[1;32m" << (long)throughput << " ops/sec\033[0m\n";
        std::cout << " TRUE BATCH RTT:      " << std::fixed << std::setprecision(4) << avg_rpc_lat_ms << " ms per network round-trip\n";
        std::cout << " AVG OP LATENCY:      " << std::fixed << std::setprecision(4) << avg_op_lat_ms << " ms per individual operation\n";
        std::cout << "============================================================\n\n";
    }
};

std::string FetchJWT(kv::KVService::Stub* stub) {
    grpc::ClientContext context;
    kv::LoginRequest req;
    kv::LoginResponse resp;
    
    req.set_client_id("async-benchmark-node");
    req.set_api_key("initial-pass");
    
    std::cout << "Authenticating with Server..." << std::endl;
    
    // Set a timeout for the login request so it doesn't hang forever if server is down
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
    context.set_deadline(deadline);
    
    grpc::Status status = stub->Login(&context, req, &resp);
    
    if (status.ok() && resp.success()) {
        std::cout << "SUCCESS: Secured JWT Token." << std::endl;
        return resp.jwt_token();
    } else {
        if (status.error_code() == grpc::StatusCode::UNAVAILABLE || status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
            std::cerr << "\n\033[1;31mCRITICAL ERROR: Target server is offline or unreachable over the network.\033[0m" << std::endl;
            std::cerr << "Make sure the server is bound to 0.0.0.0 (not 127.0.0.1) and check your IP address." << std::endl;
        } else {
            std::cerr << "CRITICAL LOGIN ERROR! status: " << status.error_message() << " (code: " << status.error_code() << ") msg: " << resp.error_message() << std::endl;
        }
        exit(1);
    }
}

void Worker(kv::KVService::Stub* stub, Benchmarker* bm, int inflight, int batch_sz, const std::string& jwt_token, int node_id) {
    grpc::CompletionQueue cq;
    long local_puts = 0, local_gets = 0, local_deletes = 0, local_errors = 0;
    long local_latency_sum_us = 0;

    // Guaranteed unique seed across different distributed clients and threads
    uint32_t seed = std::hash<std::thread::id>{}(std::this_thread::get_id()) + 
                    std::chrono::high_resolution_clock::now().time_since_epoch().count() + node_id;
                    
    auto fast_rand = [&]() -> uint32_t {
        seed ^= seed << 13; seed ^= seed >> 17; seed ^= seed << 5;
        return seed;
    };

    // Partition key spaces safely by Node ID to avoid unintended locking collisions
    uint32_t key_prefix_multiplier = (node_id * 5000000); 

    const std::string preallocated_val("v");

    auto spawn = [&]() {
        if (!bm->running) return;
        auto* call = new AsyncCall;
        
        call->ctx.AddMetadata("authorization", jwt_token);

        kv::BatchRequest req;
        for(int i=0; i<batch_sz; i++) {
            auto* e = req.add_entries();
            char key_buf[32];
            // E.g., Node 1 uses k_5000000 to k_9999999
            uint32_t random_id = key_prefix_multiplier + (fast_rand() % 5000000);
            snprintf(key_buf, sizeof(key_buf), "k_%u", random_id);
            e->set_key(key_buf);
            
            int op = fast_rand() % 3;
            e->set_type((kv::OpType)op);
            if(op == 0) e->set_value(preallocated_val); 
        }
        
        call->start = std::chrono::steady_clock::now();
        call->reader = stub->PrepareAsyncExecuteBatch(&call->ctx, req, &cq);
        call->reader->StartCall();
        call->reader->Finish(&call->reply, &call->status, (void*)call);
    };

    for (int i = 0; i < inflight; i++) spawn();

    void* tag; bool ok;
    while (cq.Next(&tag, &ok)) {
        AsyncCall* call = static_cast<AsyncCall*>(tag);
        if (ok && call->status.ok()) {
            auto now = std::chrono::steady_clock::now();
            local_latency_sum_us += std::chrono::duration_cast<std::chrono::microseconds>(now - call->start).count();
            local_puts += (batch_sz / 3);
            local_gets += (batch_sz / 3);
            local_deletes += (batch_sz / 3);
        } else {
            local_errors++;
        }
        
        delete call;
        if (bm->running) {
            spawn();
        } else {
            cq.Shutdown();
        }
    }

    bm->puts += local_puts;
    bm->gets += local_gets;
    bm->deletes += local_deletes;
    bm->errors += local_errors;
    bm->latency_sum_us += local_latency_sum_us;
}

void PrefeedWorker(kv::KVService::Stub* stub, const std::string& jwt_token, int thread_id, int num_threads, int total_keys, int node_id) {
    grpc::CompletionQueue cq;
    int keys_per_thread = total_keys / num_threads;
    
    // Offset standard keys by Node ID space
    int start_idx = (thread_id * keys_per_thread) + (node_id * 5000000); 
    int end_idx = start_idx + keys_per_thread;
    int batch_sz = 500;
    
    // Move string allocation out of inner loop
    const std::string prefeed_val(100, 'v');
    
    auto spawn = [&](int current_idx) {
        if (current_idx >= end_idx) return false;
        auto* call = new AsyncCall;
        call->ctx.AddMetadata("authorization", jwt_token);

        kv::BatchRequest req;
        for (int i = 0; i < batch_sz && current_idx + i < end_idx; i++) {
            auto* e = req.add_entries();
            char key_buf[32];
            snprintf(key_buf, sizeof(key_buf), "k_%d", current_idx + i);
            e->set_key(key_buf);
            e->set_type(kv::PUT);
            e->set_value(prefeed_val); 
        }
        
        call->reader = stub->PrepareAsyncExecuteBatch(&call->ctx, req, &cq);
        call->reader->StartCall();
        call->reader->Finish(&call->reply, &call->status, (void*)call);
        return true;
    };

    int inflight = 50;
    int dispatched = start_idx;
    for (int i = 0; i < inflight; i++) {
        if (spawn(dispatched)) dispatched += batch_sz;
    }

    void* tag; bool ok;
    while (cq.Next(&tag, &ok)) {
        AsyncCall* call = static_cast<AsyncCall*>(tag);
        delete call;
        if (dispatched < end_idx) {
            spawn(dispatched);
            dispatched += batch_sz;
        } else {
            cq.Shutdown();
        }
    }
}

// Global hook for graceful shutdown
Benchmarker* global_bm = nullptr;
void signal_handler(int signal) {
    if (global_bm) {
        std::cout << "\n\n[!] Caught termination signal. Gracefully stopping benchmark...\n";
        global_bm->running = false;
    }
}

int main(int argc, char** argv) {
    int threads = 16;
    int batch_sz = 500;
    int inflight = 100;
    std::string target_str = "localhost:50051";
    int duration = 600;
    int node_id = 1;
    bool do_prefeed = false;

    // Very basic named argument parser
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--threads" && i + 1 < argc) threads = std::stoi(argv[++i]);
        else if (arg == "--batch" && i + 1 < argc) batch_sz = std::stoi(argv[++i]);
        else if (arg == "--inflight" && i + 1 < argc) inflight = std::stoi(argv[++i]);
        else if (arg == "--target" && i + 1 < argc) target_str = argv[++i];
        else if (arg == "--duration" && i + 1 < argc) duration = std::stoi(argv[++i]);
        else if (arg == "--node-id" && i + 1 < argc) node_id = std::stoi(argv[++i]);
        else if (arg == "--prefeed") do_prefeed = true;
        else {
            std::cerr << "Usage: ./distributed_load_client [--threads <int>] [--batch <int>] [--inflight <int>] [--target <IP:PORT>] [--duration <int>] [--node-id <int>] [--prefeed]\n";
            return 1;
        }
    }

    signal(SIGINT, signal_handler);

    grpc::SslCredentialsOptions ssl_opts;
    ssl_opts.pem_root_certs = ReadFile("server.crt"); 

    grpc::ChannelArguments args;
    args.SetSslTargetNameOverride("localhost");

    auto channel = grpc::CreateCustomChannel(target_str, grpc::SslCredentials(ssl_opts), args);
    auto stub = kv::KVService::NewStub(channel);
    
    std::string dynamic_jwt = FetchJWT(stub.get());
    
    if (do_prefeed) {
        int prefeed_keys = 1000000;
        std::cout << "[!] Node " << node_id << ": Pre-feeding database with " << prefeed_keys << " keys..." << std::endl;
        std::vector<std::thread> prefeeders;
        for (int i = 0; i < 16; i++) {
            prefeeders.emplace_back(PrefeedWorker, stub.get(), dynamic_jwt, i, 16, prefeed_keys, node_id);
        }
        for (auto& t : prefeeders) t.join();
        std::cout << "[!] Pre-feed complete." << std::endl;
    } else {
        std::cout << "[!] Skipping pre-feed. Benchmarking target: " << target_str << std::endl;
    }

    std::cout << "[!] Distributed Client Set as Node " << node_id << std::endl;
    Benchmarker bm;
    global_bm = &bm;
    std::vector<std::thread> workers;

    for (int i = 0; i < threads; i++)
        workers.emplace_back(Worker, stub.get(), &bm, inflight, batch_sz, dynamic_jwt, node_id);

    std::cout << "\n[!] Benchmark started. Running for " << duration << " seconds (Press Ctrl+C to stop early)...\n";
    
    // Graceful sleep loop watching running flag
    for(int i = 0; i < duration; i++) {
        if (!bm.running) break;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    bm.running = false; 

    std::cout << "[!] Aggregating high-speed thread data..." << std::endl;
    for (auto& t : workers) if(t.joinable()) t.join(); 

    bm.print_final_stats(threads, batch_sz, duration, target_str);

    return 0;
}
```

---

## 3. Complete Setup: From Zero on Each Laptop

### Step 1: Install Dependencies (Run on ALL 3 laptops)

All 3 laptops must have the same build tools. Run these commands on each:
```bash
# Install build tools and gRPC dependencies
sudo apt update && sudo apt install -y \
    cmake git build-essential \
    libssl-dev libprotobuf-dev protobuf-compiler \
    libgrpc++-dev libgrpc-dev protobuf-compiler-grpc
```

---

### Step 2: Copy Project Files

You need to copy the `kv_server` project folder from your main laptop to each other laptop.

**Option A: Using SCP (Easiest)**
```bash
# Run this from YOUR laptop to push to the AMD Ryzen App Server
scp -r /home/anubhav/kv_server user@<AMD_LAPTOP_IP>:~/kv_server

# Push to i3 Client Laptop
scp -r /home/anubhav/kv_server user@<I3_LAPTOP_IP>:~/kv_server
```

**Option B: Using a USB Drive**
Copy the entire `kv_server` folder to a USB drive. Paste it to `~/kv_server` on both laptops.

---

### Step 3: Build on Each Laptop

After copying the files, run this on **each laptop** to compile the correct binary:

```bash
cd ~/kv_server
mkdir -p build && cd build
cmake ..
make distributed_load_client   # On the Client i3 laptop only
```

> **NOTE:** The App Server (AMD) and Storage Server (i5) also need to be compiled on their respective laptops using `make distributed_app_server` and `make storage_server`. The CMakeLists.txt needs to be updated to include these new targets.

---

### Step 4: Find IP Addresses

On **each laptop**, run the following to find its local IP:
```bash
ip addr show | grep 'inet ' | grep -v '127.0.0.1'
```
Look for a number like `192.168.x.x`. Write it down for each machine.

---

### Step 5: Update the App Server IP

On the **AMD Ryzen App Server** laptop, open `server/distributed_app_server.cpp` and change this line:
```cpp
// Line 9 — update to the i5 Storage Server's LAN IP
const std::string INTERNAL_STORAGE_IP = "192.168.1.XXX:50052";
```
Replace `192.168.1.XXX` with the real IP of the i5 laptop before compiling.

---

### Step 6: Copy the TLS Certificate to the Client

The `server.crt` file must be on the Client laptop alongside the binary. Run this from your main laptop:
```bash
scp /home/anubhav/kv_server/server.crt user@<I3_LAPTOP_IP>:~/kv_server/
```

---

### Step 7: Launch Everything (In Order!)

**Terminal on Laptop 3 (i5 Storage Server) — Start FIRST:**
```bash
cd ~/kv_server
mkdir -p data
./storage_server
# Expected output: >>> INTERNAL STORAGE NODE (RocksDB) Live on 0.0.0.0:50052 <<<
```

**Terminal on Laptop 2 (AMD App Server) — Start SECOND:**
```bash
cd ~/kv_server
./distributed_app_server
# Expected output: >>> PUBLIC APP SERVER (TLS/JWT) Live on 0.0.0.0:50051 <<<
```

**Terminal on Laptop 1 (i3 Client) — Start LAST:**
```bash
cd ~/kv_server

# Replace 192.168.1.XXX with the AMD App Server's real IP
./distributed_load_client --threads 4 --batch 1000 --inflight 20 \
    --target 192.168.1.XXX:50051 --duration 60 --node-id 1
```

---

## 4. Expected Output on the Client

```
Connecting to App Server at: 192.168.1.45:50051
Authenticating with Server...
SUCCESS: Secured JWT Token.
[!] Benchmark started. Running for 60 seconds...

================== FINAL BENCHMARK REPORT ==================
METRIC                        VALUE
------------------------------------------------------------
Target Server                 192.168.1.45:50051
Parallel Threads              4
Batch Size (Keys/RPC)         1000
Test Duration                 60s
------------------------------------------------------------
 THROUGHPUT:          XXXXX ops/sec
 TRUE BATCH RTT:      X.XXXX ms per network round-trip
 AVG OP LATENCY:      X.XXXX ms per individual operation
============================================================
```

> **Note:** Latency will be higher than `localhost` tests because requests now travel over two network hops (Client → App Server → DB Server). Expect ~5-20ms round trips depending on your Wi-Fi quality.
