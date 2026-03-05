#include <iostream>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <iomanip>
#include <fstream>
#include <sstream>
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

    void print_final_stats(int threads, int batch_sz, int duration) {

        long p = puts.load(), g = gets.load(), d = deletes.load();
        long total = p + g + d;
        double throughput = (double)total / duration;

        // THE FIX: Calculate true average Round-Trip Time per network packet (RPC)
        long total_rpcs = total / batch_sz;
        double avg_rpc_lat_ms = (total_rpcs > 0) ? ((double)latency_sum_us / 1000.0) / total_rpcs : 0;
        
        // Amortized Per-Operation Latency (total time distributed across all keys in the batch)
        double avg_op_lat_ms = (total > 0) ? ((double)latency_sum_us / 1000.0) / total : 0;

        std::cout << "\n\033[1;36m" << "================== FINAL BENCHMARK REPORT ==================" << "\033[0m\n";
        std::cout << std::left << std::setw(30) << "METRIC" << "VALUE" << "\n";
        std::cout << "------------------------------------------------------------\n";
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

// void Worker(kv::KVService::Stub* stub, Benchmarker* bm, int inflight, int batch_sz) {
//     grpc::CompletionQueue cq;
//     auto spawn = [&]() {
//         if (!bm->running) return;
//         auto* call = new AsyncCall;
        
//         // --- THE VIP PASS (Authentication) ---
//         // Injecting the secret password into the metadata headers
//         call->ctx.AddMetadata("authorization", "rocksdb-super-secret-key-2026");
//         // -------------------------------------

//         kv::BatchRequest req;
//         for(int i=0; i<batch_sz; i++) {
//             auto* e = req.add_entries();
//             int op = rand() % 3;
//             e->set_type((kv::OpType)op);
//             e->set_key("key_" + std::to_string(rand() % 1000000));
//             if(op == 0) e->set_value("val");
//         }
//         call->start = std::chrono::steady_clock::now();
//         call->reader = stub->PrepareAsyncExecuteBatch(&call->ctx, req, &cq);
//         call->reader->StartCall();
//         call->reader->Finish(&call->reply, &call->status, (void*)call);
//     };

//     for (int i = 0; i < inflight; i++) spawn();

//     void* tag; bool ok;
//     while (cq.Next(&tag, &ok)) {
//         AsyncCall* call = static_cast<AsyncCall*>(tag);
//         if (ok && call->status.ok()) {
//             auto now = std::chrono::steady_clock::now();
//             bm->latency_sum_us += std::chrono::duration_cast<std::chrono::microseconds>(now - call->start).count();
//             bm->puts += (batch_sz / 3);
//             bm->gets += (batch_sz / 3);
//             bm->deletes += (batch_sz / 3);
//         } else { 
//             bm->errors++; // If the server rejects our password, it counts as an error here!
//         }
//         delete call;
//         if (bm->running) spawn();
//         else break;
//     }
// }




// update 2 (working with 6lakh tps )

// void Worker(kv::KVService::Stub* stub, Benchmarker* bm, int inflight, int batch_sz) {
//     grpc::CompletionQueue cq;
//     auto spawn = [&]() {
//         if (!bm->running) return;
//         auto* call = new AsyncCall;
        
//         call->ctx.AddMetadata("authorization", "rocksdb-super-secret-key-2026");

//         kv::BatchRequest req;
//         for(int i=0; i<batch_sz; i++) {
//             auto* e = req.add_entries();
            
//             // --- INJECTING CONFLICTS ---
//             // 10% of the time, hit the "hot key" to force race conditions
//             if (rand() % 100 < 10) {
//                 e->set_key("global_hot_key");
//             } else {
//                 e->set_key("key_" + std::to_string(rand() % 1000000));
//             }
            
//             int op = rand() % 3;
//             e->set_type((kv::OpType)op);
//             if(op == 0) e->set_value("val");
//         }
        
//         call->start = std::chrono::steady_clock::now();
//         call->reader = stub->PrepareAsyncExecuteBatch(&call->ctx, req, &cq);
//         call->reader->StartCall();
//         call->reader->Finish(&call->reply, &call->status, (void*)call);
//     };

//     for (int i = 0; i < inflight; i++) spawn();

//     void* tag; bool ok;
//     while (cq.Next(&tag, &ok)) {
//         AsyncCall* call = static_cast<AsyncCall*>(tag);
//         if (ok && call->status.ok()) {
//             auto now = std::chrono::steady_clock::now();
//             bm->latency_sum_us += std::chrono::duration_cast<std::chrono::microseconds>(now - call->start).count();
//             bm->puts += (batch_sz / 3);
//             bm->gets += (batch_sz / 3);
//             bm->deletes += (batch_sz / 3);
//         } else if (call->status.error_code() == grpc::StatusCode::ABORTED) {
//             // COUNT THE RACE CONDITIONS PREVENTED
//             bm->errors++; 
//         } else {
//             bm->errors++;
//         }
        
//         delete call;
//         if (bm->running) spawn();
//         else break;
//     }
// }
// int main(int argc, char** argv) {
//     int threads = (argc > 1) ? std::stoi(argv[1]) : 8;
//     int batch_sz = (argc > 2) ? std::stoi(argv[2]) : 100;
//     int inflight = (argc > 3) ? std::stoi(argv[3]) : 500;
//     int duration = 300;

//     // --- SSL CONFIGURATION START ---
//     grpc::SslCredentialsOptions ssl_opts;
//     // We must load the server's public certificate so the client knows it can trust the server
//     ssl_opts.pem_root_certs = ReadFile("server.crt"); 
//     // --- SSL CONFIGURATION END ---

//     // Connect using SECURE Credentials instead of Insecure!
//     auto channel = grpc::CreateChannel("localhost:50051", grpc::SslCredentials(ssl_opts));
//     auto stub = kv::KVService::NewStub(channel);
    
//     Benchmarker bm;
//     std::vector<std::thread> workers;

//     for (int i = 0; i < threads; i++)
//         workers.emplace_back(Worker, stub.get(), &bm, inflight, batch_sz);

//     bm.print_final_stats(threads, batch_sz, duration);

//     for (auto& t : workers) if(t.joinable()) t.join();
//     return 0;
// }




// --- NEW: FETCH THE JWT FROM THE SERVER ---
// std::string FetchJWT(kv::KVService::Stub* stub) {
//     grpc::ClientContext context;
//     kv::LoginRequest req;
//     kv::LoginResponse resp;
    
//     req.set_client_id("async-benchmark-node");
//     req.set_api_key("initial-pass");
    
//     std::cout << "Authenticating with Server..." << std::endl;
//     grpc::Status status = stub->Login(&context, req, &resp);
    
//     if (status.ok() && resp.success()) {
//         std::cout << "SUCCESS: Secured JWT Token." << std::endl;
//         return resp.jwt_token();
//     } else {
//         // --- THE FIX: Reveal the actual gRPC network error ---
//         if (!status.ok()) {
//             std::cerr << "CRITICAL LOGIN ERROR (Network/gRPC): " << status.error_message() << " (Code: " << status.error_code() << ")" << std::endl;
//         } else {
//             std::cerr << "CRITICAL LOGIN ERROR (Server Rejected): " << resp.error_message() << std::endl;
//         }
//         exit(1);
//     }
// }

// // Updated Worker to accept the JWT token
// void Worker(kv::KVService::Stub* stub, Benchmarker* bm, int inflight, int batch_sz, const std::string& jwt_token) {
//     grpc::CompletionQueue cq;
//     auto spawn = [&]() {
//         if (!bm->running) return;
//         auto* call = new AsyncCall;
        
//         // --- SECURE JWT INJECTION ---
//         // Dynamically add the token we received during Login
//         call->ctx.AddMetadata("authorization", jwt_token);

//         kv::BatchRequest req;
        
//         // Fast, lock-free random number generation for the client
//         static thread_local unsigned int seed = std::hash<std::thread::id>{}(std::this_thread::get_id());
        
//         for(int i=0; i<batch_sz; i++) {
//             auto* e = req.add_entries();
            
//             // NO STRING CONCATENATION OR std::to_string. 
//             // Use a simple, fast integer-based key to avoid heap allocation overhead.
//             char key_buf[32];
//             snprintf(key_buf, sizeof(key_buf), "k_%u", rand_r(&seed) % 5000000);
//             e->set_key(key_buf);
            
//             int op = rand_r(&seed) % 3;
//             e->set_type((kv::OpType)op);
//             if(op == 0) e->set_value("v"); // Keep payload tiny for max throughput
//         }
        
//         call->start = std::chrono::steady_clock::now();
//         call->reader = stub->PrepareAsyncExecuteBatch(&call->ctx, req, &cq);
//         call->reader->StartCall();
//         call->reader->Finish(&call->reply, &call->status, (void*)call);
//     };

//     for (int i = 0; i < inflight; i++) spawn();

//     void* tag; bool ok;
//     while (cq.Next(&tag, &ok)) {
//         AsyncCall* call = static_cast<AsyncCall*>(tag);
//         if (ok && call->status.ok()) {
//             auto now = std::chrono::steady_clock::now();
//             bm->latency_sum_us += std::chrono::duration_cast<std::chrono::microseconds>(now - call->start).count();
//             bm->puts += (batch_sz / 3);
//             bm->gets += (batch_sz / 3);
//             bm->deletes += (batch_sz / 3);
//         } else if (call->status.error_code() == grpc::StatusCode::ABORTED) {
//             // COUNT THE RACE CONDITIONS PREVENTED
//             bm->errors++; 
//         } else {
//             // Unauthenticated errors or network drops will end up here
//             bm->errors++;
//         }
        
//         delete call;
//         if (bm->running) spawn();
//         else break;
//     }
// }

// int main(int argc, char** argv) {
//     int threads = (argc > 1) ? std::stoi(argv[1]) : 16;
//     int batch_sz = (argc > 2) ? std::stoi(argv[2]) : 500;
//     // Default inflight limit reduced to 50 for sane tail latency
//     int inflight = (argc > 3) ? std::stoi(argv[3]) : 100;
//     int duration =10;

//     // --- SSL CONFIGURATION START ---
//     grpc::SslCredentialsOptions ssl_opts;
//     ssl_opts.pem_root_certs = ReadFile("server.crt"); 
//     // --- SSL CONFIGURATION END ---

//     auto channel = grpc::CreateChannel("localhost:50051", grpc::SslCredentials(ssl_opts));
//     auto stub = kv::KVService::NewStub(channel);
    
//     // --- STEP 1: LOGIN ---
//     std::string dynamic_jwt = FetchJWT(stub.get());
    
//     Benchmarker bm;
//     std::vector<std::thread> workers;

//     // --- STEP 2: LAUNCH WORKERS WITH JWT ---
//     for (int i = 0; i < threads; i++)
//         workers.emplace_back(Worker, stub.get(), &bm, inflight, batch_sz, dynamic_jwt);

//     bm.print_final_stats(threads, batch_sz, duration);

//     for (auto& t : workers) if(t.joinable()) t.join();
//     return 0;
// }




std::string FetchJWT(kv::KVService::Stub* stub) {
    grpc::ClientContext context;
    kv::LoginRequest req;
    kv::LoginResponse resp;
    
    req.set_client_id("async-benchmark-node");
    req.set_api_key("initial-pass");
    
    std::cout << "Authenticating with Server..." << std::endl;
    grpc::Status status = stub->Login(&context, req, &resp);
    
    if (status.ok() && resp.success()) {
        std::cout << "SUCCESS: Secured JWT Token." << std::endl;
        return resp.jwt_token();
    } else {
        std::cerr << "CRITICAL LOGIN ERROR! status: " << status.error_message() << " (code: " << status.error_code() << ") msg: " << resp.error_message() << std::endl;
        exit(1);
    }
}

void Worker(kv::KVService::Stub* stub, Benchmarker* bm, int inflight, int batch_sz, const std::string& jwt_token) {
    grpc::CompletionQueue cq;
    
    // Private thread-local counters
    long local_puts = 0, local_gets = 0, local_deletes = 0, local_errors = 0;
    long local_latency_sum_us = 0;

    // Fast PRNG
    uint32_t seed = std::hash<std::thread::id>{}(std::this_thread::get_id());
    auto fast_rand = [&]() -> uint32_t {
        seed ^= seed << 13; seed ^= seed >> 17; seed ^= seed << 5;
        return seed;
    };

    auto spawn = [&]() {
        if (!bm->running) return;
        auto* call = new AsyncCall;
        
        call->ctx.AddMetadata("authorization", jwt_token);

        kv::BatchRequest req;
        for(int i=0; i<batch_sz; i++) {
            auto* e = req.add_entries();
            char key_buf[32];
            snprintf(key_buf, sizeof(key_buf), "k_%u", fast_rand() % 5000000);
            e->set_key(key_buf);
            
            int op = fast_rand() % 3;
            e->set_type((kv::OpType)op);
            if(op == 0) e->set_value("v"); 
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
            // Signal the queue to shut down so we don't hang!
            cq.Shutdown();
        }
    }

    // Flush private counters to global atomic ONCE at the very end
    bm->puts += local_puts;
    bm->gets += local_gets;
    bm->deletes += local_deletes;
    bm->errors += local_errors;
    bm->latency_sum_us += local_latency_sum_us;
}

void PrefeedWorker(kv::KVService::Stub* stub, const std::string& jwt_token, int thread_id, int num_threads, int total_keys) {
    grpc::CompletionQueue cq;
    int keys_per_thread = total_keys / num_threads;
    int start_idx = thread_id * keys_per_thread;
    int end_idx = start_idx + keys_per_thread;
    int batch_sz = 500;
    
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
            e->set_value(std::string(100, 'v')); // 100-byte value
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
            cq.Shutdown(); // ensure we exit cleanly
        }
    }
}

int main(int argc, char** argv) {
    int threads = (argc > 1) ? std::stoi(argv[1]) : 16;
    int batch_sz = (argc > 2) ? std::stoi(argv[2]) : 500;
    int inflight = (argc > 3) ? std::stoi(argv[3]) : 100;
    int duration = 3600; // hour

    bool do_prefeed = false;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--prefeed") {
            do_prefeed = true;
            break;
        }
    }

    grpc::SslCredentialsOptions ssl_opts;
    ssl_opts.pem_root_certs = ReadFile("server.crt"); 

    auto channel = grpc::CreateChannel("localhost:50051", grpc::SslCredentials(ssl_opts));
    auto stub = kv::KVService::NewStub(channel);
    
    std::string dynamic_jwt = FetchJWT(stub.get());
    
    // --- PRE-FEED PHASE ---
    if (do_prefeed) {
        int prefeed_keys = 1000000;
        std::cout << "[!] Pre-feeding database with " << prefeed_keys << " keys..." << std::endl;
        std::vector<std::thread> prefeeders;
        for (int i = 0; i < 16; i++) {
            prefeeders.emplace_back(PrefeedWorker, stub.get(), dynamic_jwt, i, 16, prefeed_keys);
        }
        for (auto& t : prefeeders) t.join();
        std::cout << "[!] Pre-feed complete. Database now contains " << prefeed_keys << " real keys." << std::endl;
    } else {
        std::cout << "[!] Skipping pre-feed. Benchmarking existing clustered database..." << std::endl;
    }
    // ----------------------

    Benchmarker bm;
    std::vector<std::thread> workers;

    for (int i = 0; i < threads; i++)
        workers.emplace_back(Worker, stub.get(), &bm, inflight, batch_sz, dynamic_jwt);

    // --- THE FIX: WAIT BEFORE PRINTING ---
    std::cout << "\n[!] Benchmark started. Running for " << duration << " seconds...\n";
    std::this_thread::sleep_for(std::chrono::seconds(duration));
    bm.running = false; // Stop the threads

    std::cout << "[!] Aggregating high-speed thread data..." << std::endl;
    for (auto& t : workers) if(t.joinable()) t.join(); // Wait for them to flush

    // NOW print the real data
    bm.print_final_stats(threads, batch_sz, duration);

    return 0;
}