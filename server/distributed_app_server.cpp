#include <iostream>
#include <vector>
#include <thread>
#include <fstream>
#include <grpcpp/grpcpp.h>
#include "kv.grpc.pb.h"

// Hardcoded for the internal LAN network (Change this to the i5 Laptop's Real IP)
const std::string INTERNAL_STORAGE_IP = "192.168.1.XXX:50052"; 

const std::string AUTH_HEADER = "authorization";
const std::string AUTH_TOKEN = "rocksdb-super-secret-key-2026";

std::string ReadFile(const std::string& filename) {
    std::ifstream ifs(filename);
    if (!ifs.is_open()) {
        std::cerr << "CRITICAL ERROR: Failed to open " << filename << std::endl;
        exit(1);
    }
    return std::string((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
}

class AppServerImpl final {
public:
    void Run() {
        // --- 1. CONNECT TO INTERNAL STORAGE SERVER ---
        grpc::ChannelArguments args;
        args.SetMaxReceiveMessageSize(128 * 1024 * 1024);
        
        // Private LAN connection, so no TLS here
        auto channel = grpc::CreateCustomChannel(INTERNAL_STORAGE_IP, grpc::InsecureChannelCredentials(), args);
        storage_stub_ = kv::KVService::NewStub(channel);

        // --- 2. START PUBLIC-FACING SECURE SERVER ---
        grpc::SslServerCredentialsOptions ssl_opts;
        ssl_opts.pem_key_cert_pairs.push_back({ReadFile("../server.key"), ReadFile("../server.crt")});

        grpc::ServerBuilder builder;
        builder.SetMaxReceiveMessageSize(128 * 1024 * 1024);
        builder.AddListeningPort("0.0.0.0:50051", grpc::SslServerCredentials(ssl_opts));
        builder.RegisterService(&service_);

        int threads = std::thread::hardware_concurrency();
        for (int i = 0; i < threads; i++) cqs_.emplace_back(builder.AddCompletionQueue());

        server_ = builder.BuildAndStart();
        std::cout << ">>> PUBLIC APP SERVER (TLS/JWT) Live on 0.0.0.0:50051 <<<" << std::endl;
        std::cout << "Routing internal database traffic to: " << INTERNAL_STORAGE_IP << std::endl;

        std::vector<std::thread> workers;
        for (auto& cq : cqs_) workers.emplace_back(&AppServerImpl::HandleRpcs, this, cq.get());
        for (auto& t : workers) t.join();
    }

private:
    struct CallData {
        kv::KVService::AsyncService* service;
        grpc::ServerCompletionQueue* cq;
        kv::KVService::Stub* storage_stub;
        
        grpc::ServerContext ctx;
        kv::BatchRequest req;
        kv::BatchResponse resp;
        grpc::ServerAsyncResponseWriter<kv::BatchResponse> responder;
        enum { CREATE, PROCESS, FINISH } status;

        CallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c, kv::KVService::Stub* stub)
            : service(s), cq(c), storage_stub(stub), responder(&ctx), status(CREATE) { Proceed(); }

        void Proceed() {
            if (status == CREATE) {
                status = PROCESS;
                service->RequestExecuteBatch(&ctx, &req, &responder, cq, cq, this);
            } else if (status == PROCESS) {
                new CallData(service, cq, storage_stub);

                // --- VERIFY JWT SECURITY TOKEN ---
                auto& client_metadata = ctx.client_metadata();
                auto it = client_metadata.find(AUTH_HEADER);
                if (it == client_metadata.end() || it->second != AUTH_TOKEN) {
                    status = FINISH;
                    responder.Finish(resp, grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Denied"), this);
                    return;
                }

                // --- ROUTE TO INTERNAL STORAGE NETWORK ---
                grpc::ClientContext internal_ctx;
                grpc::Status s = storage_stub->ExecuteBatch(&internal_ctx, req, &resp);

                status = FINISH;
                responder.Finish(resp, s, this);
            } else { 
                delete this; 
            }
        }
    };

    void HandleRpcs(grpc::ServerCompletionQueue* cq) {
        new CallData(&service_, cq, storage_stub_.get());
        void* tag; bool ok;
        while (cq->Next(&tag, &ok)) {
            if (ok) static_cast<CallData*>(tag)->Proceed();
        }
    }

    std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
    kv::KVService::AsyncService service_;
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<kv::KVService::Stub> storage_stub_;
};

int main() { AppServerImpl s; s.Run(); return 0; }
