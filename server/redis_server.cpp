#include <iostream>
#include <vector>
#include <thread>
#include <fstream>
#include <unordered_map>
#include <chrono>
#include <csignal>
#include <atomic>
#include <mutex>

#include <hiredis/hiredis.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <jwt-cpp/jwt.h>

#include "kv.grpc.pb.h"

const std::string MASTER_SECRET = "super-secret-server-key-256";

// Per-thread Redis connection pool
thread_local redisContext* tl_redis = nullptr;

static std::string g_redis_host = "127.0.0.1";
static int         g_redis_port = 6379;

redisContext* GetRedis() {
    if (!tl_redis) {
        tl_redis = redisConnect(g_redis_host.c_str(), g_redis_port);
        if (!tl_redis || tl_redis->err) {
            std::cerr << "Redis connect error: "
                      << (tl_redis ? tl_redis->errstr : "null ctx") << std::endl;
            exit(1);
        }
    }
    return tl_redis;
}

std::string ReadFile(const std::string& filename) {
    std::ifstream ifs(filename);
    if (!ifs.is_open()) {
        std::cerr << "CRITICAL ERROR: Failed to open " << filename << std::endl;
        exit(1);
    }
    return std::string((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
}

bool VerifyJWTCached(const std::string& token_str) {
    thread_local std::unordered_map<std::string,
        std::chrono::time_point<std::chrono::steady_clock>> local_cache;

    if (local_cache.size() > 10000) local_cache.clear();

    auto now = std::chrono::steady_clock::now();
    auto it = local_cache.find(token_str);
    if (it != local_cache.end()) {
        if (now < it->second) return true;
        local_cache.erase(it);
    }

    try {
        auto decoded = jwt::decode(token_str);
        auto verifier = jwt::verify()
            .allow_algorithm(jwt::algorithm::hs256{MASTER_SECRET})
            .with_issuer("kv_server");
        verifier.verify(decoded);
        local_cache[token_str] = now + std::chrono::seconds(60);
        return true;
    } catch (...) {
        return false;
    }
}

class CallDataBase {
public:
    virtual void Proceed() = 0;
    virtual ~CallDataBase() = default;
};

class ServerImpl final {
public:
    ~ServerImpl() { Shutdown(); }

    void Run() {
        int num_cores = std::thread::hardware_concurrency();

        grpc::SslServerCredentialsOptions ssl_opts;
        ssl_opts.pem_key_cert_pairs.push_back({ReadFile("server.key"), ReadFile("server.crt")});

        grpc::ServerBuilder builder;
        builder.SetMaxReceiveMessageSize(128 * 1024 * 1024);
        builder.AddListeningPort("0.0.0.0:50051", grpc::SslServerCredentials(ssl_opts));
        builder.RegisterService(&service_);

        for (int i = 0; i < num_cores; i++)
            cqs_.emplace_back(builder.AddCompletionQueue());

        server_ = builder.BuildAndStart();
        std::cout << ">>> REDIS TURBO MODE ACTIVE on port 50051 <<<" << std::endl;

        int threads_per_core = 8;
        for (auto& cq : cqs_) {
            for (int i = 0; i < threads_per_core; i++) {
                workers_.emplace_back([this, cq_ptr = cq.get()]() {
                    // Pre-warm Redis connection for this thread
                    GetRedis();
                    for (int j = 0; j < 500; ++j) {
                        new LoginCallData(&service_, cq_ptr);
                        new BatchCallData(&service_, cq_ptr);
                    }
                    void* tag; bool ok;
                    while (cq_ptr->Next(&tag, &ok)) {
                        if (ok) static_cast<CallDataBase*>(tag)->Proceed();
                        else    delete static_cast<CallDataBase*>(tag);
                    }
                });
            }
        }
        server_->Wait();
    }

    void Shutdown() {
        if (!is_shutdown_.exchange(true)) {
            std::cout << "\nInitiating graceful shutdown..." << std::endl;
            if (server_) server_->Shutdown();
            for (auto& cq : cqs_) cq->Shutdown();
            for (auto& t : workers_) if (t.joinable()) t.join();
            std::cout << "Redis server safely terminated." << std::endl;
        }
    }

private:
    // ── LOGIN ────────────────────────────────────────────────────────────────
    struct LoginCallData : public CallDataBase {
        kv::KVService::AsyncService* service;
        grpc::ServerCompletionQueue* cq;
        grpc::ServerContext ctx;
        kv::LoginRequest req;
        kv::LoginResponse resp;
        grpc::ServerAsyncResponseWriter<kv::LoginResponse> responder;
        enum { CREATE, PROCESS, FINISH } status;

        LoginCallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c)
            : service(s), cq(c), responder(&ctx), status(CREATE) { Proceed(); }

        void Proceed() override {
            if (status == CREATE) {
                status = PROCESS;
                service->RequestLogin(&ctx, &req, &responder, cq, cq, this);
            } else if (status == PROCESS) {
                new LoginCallData(service, cq);
                if (req.api_key() == "initial-pass") {
                    auto token = jwt::create()
                        .set_issuer("kv_server")
                        .set_subject(req.client_id())
                        .set_issued_at(std::chrono::system_clock::now())
                        .set_expires_at(std::chrono::system_clock::now() + std::chrono::hours(1))
                        .sign(jwt::algorithm::hs256{MASTER_SECRET});
                    resp.set_success(true);
                    resp.set_jwt_token(token);
                } else {
                    resp.set_success(false);
                    resp.set_error_message("Invalid API Key");
                }
                status = FINISH;
                responder.Finish(resp, grpc::Status::OK, this);
            } else {
                delete this;
            }
        }
    };

    // ── BATCH ────────────────────────────────────────────────────────────────
    struct BatchCallData : public CallDataBase {
        kv::KVService::AsyncService* service;
        grpc::ServerCompletionQueue* cq;
        grpc::ServerContext ctx;
        kv::BatchRequest req;
        kv::BatchResponse resp;
        grpc::ServerAsyncResponseWriter<kv::BatchResponse> responder;
        enum { CREATE, PROCESS, FINISH } status;

        BatchCallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c)
            : service(s), cq(c), responder(&ctx), status(CREATE) { Proceed(); }

        void Proceed() override {
            if (status == CREATE) {
                status = PROCESS;
                service->RequestExecuteBatch(&ctx, &req, &responder, cq, cq, this);
            } else if (status == PROCESS) {
                new BatchCallData(service, cq);

                auto const& md = ctx.client_metadata();
                auto it = md.find("authorization");
                if (it == md.end() || !VerifyJWTCached(std::string(it->second.data(), it->second.size()))) {
                    status = FINISH;
                    responder.Finish(resp, grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Deny"), this);
                    return;
                }

                // --- Execute batch via Redis TRANSACTIONS (MULTI/EXEC) ---
                redisContext* rc = GetRedis();
                bool ok = true;
                int cmd_count = 0;

                // 1. Begin atomic transaction block
                redisAppendCommand(rc, "MULTI");
                cmd_count++;

                // 2. Queue all operations
                for (const auto& entry : req.entries()) {
                    if (entry.type() == kv::PUT) {
                        redisAppendCommand(rc, "SET %s %s", entry.key().c_str(), entry.value().c_str());
                        cmd_count++;
                    } else if (entry.type() == kv::DELETE) {
                        redisAppendCommand(rc, "DEL %s", entry.key().c_str());
                        cmd_count++;
                    }
                    // GET ops are read-only and skipped in this write benchmark
                }

                // 3. Commit the transaction atomically
                redisAppendCommand(rc, "EXEC");
                cmd_count++;

                // 4. Drain all replies from the pipeline
                for (int i = 0; i < cmd_count; i++) {
                    redisReply* reply = nullptr;
                    if (redisGetReply(rc, (void**)&reply) != REDIS_OK || !reply) {
                        ok = false;
                        if (reply) freeReplyObject(reply);
                        break;
                    }
                    
                    // If the EXEC command itself returned a null/error array, the transaction aborted!
                    if (i == cmd_count - 1 && reply->type == REDIS_REPLY_NIL) {
                        ok = false; // Transaction failed/aborted
                    }
                    
                    freeReplyObject(reply);
                }

                resp.set_success(ok);
                status = FINISH;
                responder.Finish(resp, grpc::Status::OK, this);
            } else {
                delete this;
            }
        }
    };

    std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
    kv::KVService::AsyncService service_;
    std::unique_ptr<grpc::Server> server_;
    std::vector<std::thread> workers_;
    std::atomic<bool> is_shutdown_{false};
};

ServerImpl* g_server = nullptr;
void HandleSignal(int) { if (g_server) g_server->Shutdown(); }

int main() {
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);
    ServerImpl s;
    g_server = &s;
    s.Run();
    return 0;
}
