#include <iostream>
#include <thread>
#include <vector>
#include <grpcpp/grpcpp.h>
#include "kv.grpc.pb.h"
#include <fstream>
#include <chrono>

// Helper to read the SSL cert
std::string ReadFile(const std::string& filename) {
    std::ifstream ifs(filename);
    if (!ifs.is_open()) {
        std::cerr << "CRITICAL ERROR: Failed to open server.crt" << std::endl;
        exit(1);
    }
    return std::string((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
}

void Attack(kv::KVService::Stub* stub) {
    for (int i = 0; i < 20; i++) { // Reduced to 20 per thread to see the retries clearly
        bool success = false;
        int retries = 0;
        int max_retries = 5;
        int backoff_ms = 10; // Initial wait time

        while (!success && retries < max_retries) {
            grpc::ClientContext ctx;
            ctx.AddMetadata("authorization", "rocksdb-super-secret-key-2026");
            
            kv::BatchRequest req;
            auto* e = req.add_entries();
            e->set_type(kv::PUT);
            e->set_key("shared_resource_01"); // The conflict zone
            e->set_value("val_" + std::to_string(rand() % 100));

            kv::BatchResponse resp;
            grpc::Status status = stub->ExecuteBatch(&ctx, req, &resp);

            if (status.ok()) {
                std::cout << "\033[1;32m[SUCCESS]\033[0m Commit allowed for request " << i << std::endl;
                success = true;
            } else if (status.error_code() == grpc::StatusCode::ABORTED) {
                // This is our Optimistic Lock catching a race condition!
                retries++;
                std::cout << "\033[1;33m[RETRYING]\033[0m Race detected on request " << i 
                          << ". Attempt " << retries << "/" << max_retries << "..." << std::endl;
                
                // Wait before trying again (Exponential Backoff)
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
                backoff_ms *= 2; // Double the wait for next time
            } else {
                std::cout << "\033[1;31m[ERROR]\033[0m Permanent Failure: " << status.error_message() << std::endl;
                break; 
            }
        }
        
        if (!success) {
            std::cout << "\033[1;41m[FAILED]\033[0m Could not resolve race after " << max_retries << " attempts." << std::endl;
        }
    }
}

int main() {
    grpc::SslCredentialsOptions ssl_opts;
    ssl_opts.pem_root_certs = ReadFile("server.crt");
    
    // Use a secure channel
    auto channel = grpc::CreateChannel("localhost:50051", grpc::SslCredentials(ssl_opts));
    auto stub = kv::KVService::NewStub(channel);

    std::cout << "Starting Conflict Test with Exponential Backoff..." << std::endl;

    std::vector<std::thread> attackers;
    // Launching 8 threads to create heavy contention on one key
    for (int i = 0; i < 8; i++) attackers.emplace_back(Attack, stub.get());
    for (auto& t : attackers) t.join();

    std::cout << "Test Complete." << std::endl;
    return 0;
}