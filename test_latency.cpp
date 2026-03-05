#include <iostream>
#include <chrono>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include "kv.grpc.pb.h"

std::string ReadFile(const std::string& filename) {
    std::ifstream ifs(filename);
    if (!ifs.is_open()) {
        std::cerr << "CRITICAL ERROR: Failed to open " << filename << std::endl;
        exit(1);
    }
    return std::string((std::istreambuf_iterator<char>(ifs)),
                       std::istreambuf_iterator<char>());
}

std::string FetchJWT(kv::KVService::Stub* stub) {
    grpc::ClientContext context;
    kv::LoginRequest req;
    kv::LoginResponse resp;
    
    req.set_client_id("test-client");
    req.set_api_key("initial-pass");
    
    grpc::Status status = stub->Login(&context, req, &resp);
    if (status.ok() && resp.success()) return resp.jwt_token();
    exit(1);
}

int main() {
    grpc::SslCredentialsOptions ssl_opts;
    ssl_opts.pem_root_certs = ReadFile("server.crt"); 

    auto channel = grpc::CreateChannel("localhost:50051", grpc::SslCredentials(ssl_opts));
    auto stub = kv::KVService::NewStub(channel);
    
    std::string jwt = FetchJWT(stub.get());

    for (int i=0; i<5; i++) {
        grpc::ClientContext ctx;
        ctx.AddMetadata("authorization", jwt);

        kv::BatchRequest req;
        auto* e = req.add_entries();
        e->set_key("ping");
        e->set_type(kv::PUT);
        e->set_value("pong");

        kv::BatchResponse resp;
        auto start = std::chrono::steady_clock::now();
        grpc::Status status = stub->ExecuteBatch(&ctx, req, &resp);
        auto end = std::chrono::steady_clock::now();

        std::cout << "Latency: " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << " us" << std::endl;
    }
    return 0;
}
