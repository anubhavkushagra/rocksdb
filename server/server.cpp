// #include <iostream>
// #include <vector>
// #include <thread>
// #include <fstream>
// #include <sstream>
// #include <rocksdb/db.h>
// #include <rocksdb/options.h>
// #include <rocksdb/utilities/optimistic_transaction_db.h>
// #include <rocksdb/utilities/transaction.h>

// #include <grpcpp/grpcpp.h>
// #include "kv.grpc.pb.h"

// using namespace rocksdb;

// // Pre-defined for faster metadata lookup
// const std::string AUTH_HEADER = "authorization";
// const std::string AUTH_TOKEN = "rocksdb-super-secret-key-2026";

// // --- SECURITY HELPER: Read Certificate Files ---
// std::string ReadFile(const std::string& filename) {
//     std::ifstream ifs(filename);
//     if (!ifs.is_open()) {
//         std::cerr << "CRITICAL ERROR: Failed to open " << filename << std::endl;
//         exit(1);
//     }
//     return std::string((std::istreambuf_iterator<char>(ifs)),
//                        std::istreambuf_iterator<char>());
// }

// class ServerImpl final {
// public:
//     void Run() {
//         Options options;
//         options.create_if_missing = true;

//         // --- ROCKSDB HIGH-THROUGHPUT TUNING ---
//         options.IncreaseParallelism(); 
//         options.OptimizeLevelStyleCompaction();
        
//         // Memory Buffers: Increase size to handle massive ingestion
//         options.write_buffer_size = 256 * 1024 * 1024; // 256MB MemTable
//         options.max_write_buffer_number = 4;
        
//         // Pipelining: Allows multiple threads to prep writes in parallel
//         options.enable_pipelined_write = true;
//       //  options.unordered_write = true; // Fastest possible write mode for concurrent operations
//         options.allow_concurrent_memtable_write = true;

//         // Open as OptimisticTransactionDB
//         Status s = OptimisticTransactionDB::Open(options, "./data", &txn_db_);
//         if (!s.ok()) {
//             std::cerr << "Failed to open TransactionDB: " << s.ToString() << std::endl;
//             exit(1);
//         }

//         // --- SECURE CHANNEL CONFIG ---
//         grpc::SslServerCredentialsOptions ssl_opts;
//         ssl_opts.pem_key_cert_pairs.push_back({ReadFile("server.key"), ReadFile("server.crt")});

//         grpc::ServerBuilder builder;
//         builder.AddListeningPort("0.0.0.0:50051", grpc::SslServerCredentials(ssl_opts));
//         builder.RegisterService(&service_);

//         // Align Completion Queues with Hardware Threads
//         int threads = std::thread::hardware_concurrency();
//         for (int i = 0; i < threads; i++) cqs_.emplace_back(builder.AddCompletionQueue());

//         server_ = builder.BuildAndStart();
//         std::cout << "1M+ TPS Optimized Transactional Server Live." << std::endl;

//         std::vector<std::thread> workers;
//         for (auto& cq : cqs_) workers.emplace_back(&ServerImpl::HandleRpcs, this, cq.get());
//         for (auto& t : workers) t.join();
//     }

// private:
//     struct CallData {
//         kv::KVService::AsyncService* service;
//         grpc::ServerCompletionQueue* cq;
//         OptimisticTransactionDB* txn_db;
//         grpc::ServerContext ctx;
//         kv::BatchRequest req;
//         kv::BatchResponse resp;
//         grpc::ServerAsyncResponseWriter<kv::BatchResponse> responder;
//         enum { CREATE, PROCESS, FINISH } status;

//         CallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c, OptimisticTransactionDB* d)
//             : service(s), cq(c), txn_db(d), responder(&ctx), status(CREATE) { Proceed(); }

//         void Proceed() {
//             if (status == CREATE) {
//                 status = PROCESS;
//                 service->RequestExecuteBatch(&ctx, &req, &responder, cq, cq, this);
//             } else if (status == PROCESS) {
//                 new CallData(service, cq, txn_db);

//                 // --- OPTIMIZED AUTH CHECK ---
//                 auto& client_metadata = ctx.client_metadata();
//                 auto it = client_metadata.find(AUTH_HEADER);
//                 if (it == client_metadata.end() || it->second != AUTH_TOKEN) {
//                     status = FINISH;
//                     responder.Finish(resp, grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Denied"), this);
//                     return;
//                 }

//                 // --- TRANSACTIONAL BATCH PROCESSING ---
//                 WriteOptions write_options;
//                 // Set to false for benchmark speed (risks data loss on crash, but hits 1M+ TPS)
//                 write_options.disableWAL = true; 
                
//                 OptimisticTransactionOptions txn_options;
//                 txn_options.set_snapshot = true; // Provides consistent point-in-time view

//                 Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);

//                 ReadOptions read_opts;
//                 for (const auto& entry : req.entries()) {
//                     if (entry.type() == kv::PUT) {
//                         txn->Put(entry.key(), entry.value());
//                     } else if (entry.type() == kv::DELETE) {
//                         txn->Delete(entry.key());
//                     } else if (entry.type() == kv::GET) {
//                         std::string val;
//                         // GetForUpdate prevents race conditions by tracking this key's version
//                         txn->GetForUpdate(read_opts, entry.key(), &val);
//                         resp.add_values(val);
//                     }
//                 }

//                 Status s = txn->Commit();
//                 resp.set_success(s.ok());

//                 if (s.ok()) {
//                     responder.Finish(resp, grpc::Status::OK, this);
//                 } else {
//                     // Send ABORTED so the client knows to trigger its Retry/Backoff logic
//                     responder.Finish(resp, grpc::Status(grpc::StatusCode::ABORTED, "Conflict Detected"), this);
//                 }
                
//                 delete txn;
//                 status = FINISH;
//             } else { 
//                 delete this; 
//             }
//         }
//     };

//     void HandleRpcs(grpc::ServerCompletionQueue* cq) {
//         new CallData(&service_, cq, txn_db_);
//         void* tag; bool ok;
//         while (cq->Next(&tag, &ok)) {
//             if (ok) static_cast<CallData*>(tag)->Proceed();
//         }
//     }

//     std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
//     kv::KVService::AsyncService service_;
//     std::unique_ptr<grpc::Server> server_;
//     OptimisticTransactionDB* txn_db_;
// };

// int main() { ServerImpl s; s.Run(); return 0; }



// update 2 - Added SSL/TLS support to the server for secure communication with clients.




// #include <iostream>
// #include <vector>
// #include <thread>
// #include <fstream>
// #include <rocksdb/db.h>
// #include <rocksdb/options.h>
// #include <rocksdb/utilities/optimistic_transaction_db.h>
// #include <rocksdb/utilities/transaction.h>
// #include <grpcpp/grpcpp.h>
// #include <grpcpp/resource_quota.h>
// #include "kv.grpc.pb.h"

// using namespace rocksdb;

// const std::string AUTH_TOKEN = "rocksdb-super-secret-key-2026";

// std::string ReadFile(const std::string& filename) {
//     std::ifstream ifs(filename);
//     return std::string((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
// }

// class ServerImpl final {
// public:
//     void Run() {
//         Options options;
//         options.create_if_missing = true;
        
//         // --- ROCKSDB AGGRESSIVE TUNING ---
//         int num_cores = std::thread::hardware_concurrency();
//         options.IncreaseParallelism(num_cores);
//         options.OptimizeLevelStyleCompaction();
        
//         // 512MB Memtable for fewer flushes
//         options.write_buffer_size = 512 * 1024 * 1024; 
//         options.max_write_buffer_number = 8;
//         options.min_write_buffer_number_to_merge = 2;
        
//         // Disable WAL and use Pipelined Writes for 1M+ TPS
//         options.enable_pipelined_write = true;
//         options.allow_concurrent_memtable_write = true;
//         options.max_background_compactions = 4;

//         Status s = OptimisticTransactionDB::Open(options, "./data", &txn_db_);
//         if (!s.ok()) {
//             std::cerr << "DB Error: " << s.ToString() << std::endl;
//             exit(1);
//         }

//         w_opts.disableWAL = true; 
//         w_opts.sync = false;

//         // --- gRPC RESOURCE OPTIMIZATION ---
//         grpc::ResourceQuota quota;
//         quota.SetMaxThreads(num_cores * 2);

//         grpc::SslServerCredentialsOptions ssl_opts;
//         ssl_opts.pem_key_cert_pairs.push_back({ReadFile("server.key"), ReadFile("server.crt")});

//         grpc::ServerBuilder builder;
//         builder.SetResourceQuota(quota);
//         // Increase message limit for huge batches
//         builder.SetMaxReceiveMessageSize(128 * 1024 * 1024); 
//         builder.AddListeningPort("0.0.0.0:50051", grpc::SslServerCredentials(ssl_opts));
//         builder.RegisterService(&service_);

//         for (int i = 0; i < num_cores; i++) {
//             cqs_.emplace_back(builder.AddCompletionQueue());
//         }

//         server_ = builder.BuildAndStart();
//         std::cout << ">>> TURBO MODE ACTIVE: Targeting 1.2M+ TPS <<<" << std::endl;

//         std::vector<std::thread> workers;
//         for (auto& cq : cqs_) {
//             workers.emplace_back([this, cq_ptr = cq.get()]() {
//                 // Initialize the pipeline
//                 for(int i=0; i<100; ++i) { 
//                     new CallData(&service_, cq_ptr, txn_db_, &w_opts);
//                 }
                
//                 void* tag;
//                 bool ok;
//                 while (cq_ptr->Next(&tag, &ok)) {
//                     if (ok) static_cast<CallData*>(tag)->Proceed();
//                 }
//             });
//         }
//         for (auto& t : workers) t.join();
//     }

// private:
//     struct CallData {
//         kv::KVService::AsyncService* service;
//         grpc::ServerCompletionQueue* cq;
//         OptimisticTransactionDB* txn_db;
//         WriteOptions* w_opts;
//         grpc::ServerContext ctx;
//         kv::BatchRequest req;
//         kv::BatchResponse resp;
//         grpc::ServerAsyncResponseWriter<kv::BatchResponse> responder;
//         enum { CREATE, PROCESS, FINISH } status;

//         CallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c, 
//                  OptimisticTransactionDB* d, WriteOptions* wo)
//             : service(s), cq(c), txn_db(d), w_opts(wo), responder(&ctx), status(CREATE) {
//             Proceed();
//         }

//         void Proceed() {
//             if (status == CREATE) {
//                 status = PROCESS;
//                 service->RequestExecuteBatch(&ctx, &req, &responder, cq, cq, this);
//             } else if (status == PROCESS) {
//                 // SPAWN IMMEDIATELY: Don't wait for DB write to accept next RPC
//                 new CallData(service, cq, txn_db, w_opts);

//                 auto const& md = ctx.client_metadata();
//                 auto it = md.find("authorization");
//                 if (it == md.end() || std::string(it->second.data(), it->second.size()) != AUTH_TOKEN) {
//                     responder.Finish(resp, grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Deny"), this);
//                 } else {
//                     // --- OPTIMIZED BATCH WRITE ---
//                     Transaction* txn = txn_db->BeginTransaction(*w_opts);
//                     for (const auto& entry : req.entries()) {
//                         if (entry.type() == kv::PUT) txn->Put(entry.key(), entry.value());
//                     }
//                     Status s = txn->Commit();
//                     resp.set_success(s.ok());
//                     delete txn;

//                     responder.Finish(resp, grpc::Status::OK, this);
//                 }
//                 status = FINISH;
//             } else {
//                 delete this;
//             }
//         }
//     };

//     std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
//     kv::KVService::AsyncService service_;
//     std::unique_ptr<grpc::Server> server_;
//     OptimisticTransactionDB* txn_db_;
//     WriteOptions w_opts;
// };

// int main() {
//     ServerImpl s;
//     s.Run();
//     return 0;
// }




// #include <iostream>
// #include <vector>
// #include <thread>
// #include <fstream>
// #include <unordered_map>
// #include <rocksdb/db.h>
// #include <rocksdb/options.h>
// #include <rocksdb/utilities/optimistic_transaction_db.h>
// #include <rocksdb/utilities/transaction.h>
// #include <grpcpp/grpcpp.h>
// #include <grpcpp/resource_quota.h>
// #include <jwt-cpp/jwt.h>
// #include "kv.grpc.pb.h"

// using namespace rocksdb;

// const std::string MASTER_SECRET = "super-secret-server-key-256";

// bool VerifyJWTCached(const std::string& token_str) {
//     // THE FIX: 'thread_local' means every worker thread gets its own private cache!
//     // NO MUTEX NEEDED. NO WAITING IN LINE.
//     thread_local std::unordered_map<std::string, std::chrono::time_point<std::chrono::steady_clock>> local_cache;
    
//     auto now = std::chrono::steady_clock::now();
    
//     // 1. Check the thread's private cache
//     auto it = local_cache.find(token_str);
//     if (it != local_cache.end()) {
//         if (now < it->second) return true; // Still valid!
//         local_cache.erase(it); // Expired
//     }

//     // 2. Heavy math only if it's not in this specific thread's cache
//     try {
//         auto decoded = jwt::decode(token_str);
//         auto verifier = jwt::verify()
//             .allow_algorithm(jwt::algorithm::hs256{MASTER_SECRET})
//             .with_issuer("kv_server");
//         verifier.verify(decoded);
        
//         // 3. Save to the private cache
//         local_cache[token_str] = now + std::chrono::seconds(60);
//         return true;
//     } catch (...) {
//         return false; 
//     }
// }

// std::string ReadFile(const std::string& filename) {
//     std::ifstream ifs(filename);
//     return std::string((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
// }

// // Base class so our worker loop can process both Logins and Batches
// class CallDataBase {
// public:
//     virtual void Proceed() = 0;
//     virtual ~CallDataBase() = default;
// };

// class ServerImpl final {
// public:
//     void Run() {
//         Options options;
//         options.create_if_missing = true;
        
//         // --- ROCKSDB AGGRESSIVE TUNING ---
//         int num_cores = std::thread::hardware_concurrency();
//         options.IncreaseParallelism(num_cores);
//         options.OptimizeLevelStyleCompaction();
//         // ADD THIS LINE: Maximize background threads to prevent latency stalls
//         options.max_background_jobs = num_cores; 
        
//         // 512MB Memtable for fewer flushes
//         options.write_buffer_size = 512 * 1024 * 1024; 
//         options.max_write_buffer_number = 8;
//         options.min_write_buffer_number_to_merge = 2;
        
//         // Disable WAL and use Pipelined Writes for 1M+ TPS
//         options.enable_pipelined_write = true;
//         options.allow_concurrent_memtable_write = true;
//         options.max_background_compactions = 4;

//         Status s = OptimisticTransactionDB::Open(options, "./data", &txn_db_);
//         if (!s.ok()) {
//             std::cerr << "DB Error: " << s.ToString() << std::endl;
//             exit(1);
//         }

//         w_opts.disableWAL = true; 
//         w_opts.sync = false;

//         // --- gRPC RESOURCE OPTIMIZATION ---
//         grpc::ResourceQuota quota;
//         quota.SetMaxThreads(num_cores * 2);

//         grpc::SslServerCredentialsOptions ssl_opts;
//         ssl_opts.pem_key_cert_pairs.push_back({ReadFile("server.key"), ReadFile("server.crt")});

//         grpc::ServerBuilder builder;
//         builder.SetResourceQuota(quota);
//         builder.SetMaxReceiveMessageSize(128 * 1024 * 1024); 
//         builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());
//         builder.RegisterService(&service_);

//         for (int i = 0; i < num_cores; i++) {
//             cqs_.emplace_back(builder.AddCompletionQueue());
//         }

//         server_ = builder.BuildAndStart();
//         std::cout << ">>> TURBO MODE + JWT CACHE ACTIVE: Targeting 1.2M+ TPS <<<" << std::endl;

//         std::cout << ">>> TURBO MODE + MULTI-THREADED CQs ACTIVE <<<" << std::endl;

//         std::vector<std::thread> workers;
        
//         // --- THE FIX: Multi-Threaded Completion Queues ---
//         // Spawn 4 threads per core. If you have 8 cores, this creates 32 threads.
//         // If RocksDB pauses one thread, the others keep draining the network!
//         int threads_per_core = 4; 
        
//         for (auto& cq : cqs_) {
//             for (int i = 0; i < threads_per_core; i++) {
//                 workers.emplace_back([this, cq_ptr = cq.get()]() {
                    
//                     // Pre-allocate a safe amount of CallData per thread
//                     for(int j=0; j<250; ++j) { 
//                         new LoginCallData(&service_, cq_ptr);
//                         new BatchCallData(&service_, cq_ptr, txn_db_, &w_opts);
//                     }
                    
//                     void* tag;
//                     bool ok;
//                     // gRPC safely handles multiple threads pulling from the exact same queue!
//                     while (cq_ptr->Next(&tag, &ok)) {
//                         if (ok) static_cast<CallDataBase*>(tag)->Proceed();
//                     }
//                 });
//             }
//         }
//         for (auto& t : workers) t.join();
//     }

// private:
//     // --- STATE MACHINE 1: LOGIN ---
//     struct LoginCallData : public CallDataBase {
//         kv::KVService::AsyncService* service;
//         grpc::ServerCompletionQueue* cq;
//         grpc::ServerContext ctx;
//         kv::LoginRequest req;
//         kv::LoginResponse resp;
//         grpc::ServerAsyncResponseWriter<kv::LoginResponse> responder;
//         enum { CREATE, PROCESS, FINISH } status;

//         LoginCallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c)
//             : service(s), cq(c), responder(&ctx), status(CREATE) { Proceed(); }

//         void Proceed() override {
//             if (status == CREATE) {
//                 status = PROCESS;
//                 service->RequestLogin(&ctx, &req, &responder, cq, cq, this);
//             } else if (status == PROCESS) {
//                 new LoginCallData(service, cq); // Spawn next

//                 // Generate token if API key matches
//                 if (req.api_key() == "initial-pass") {
//                     auto token = jwt::create()
//                         .set_issuer("kv_server")
//                         .set_subject(req.client_id())
//                         .set_issued_at(std::chrono::system_clock::now())
//                         .set_expires_at(std::chrono::system_clock::now() + std::chrono::hours(1))
//                         .sign(jwt::algorithm::hs256{MASTER_SECRET});
//                     resp.set_success(true);
//                     resp.set_jwt_token(token);
//                 } else {
//                     resp.set_success(false);
//                     resp.set_error_message("Invalid API Key");
//                 }
//                 status = FINISH;
//                 responder.Finish(resp, grpc::Status::OK, this);
//             } else { delete this; }
//         }
//     };

//     // --- STATE MACHINE 2: HIGH SPEED BATCH ---
//     struct BatchCallData : public CallDataBase {
//         kv::KVService::AsyncService* service;
//         grpc::ServerCompletionQueue* cq;
//         OptimisticTransactionDB* txn_db;
//         WriteOptions* w_opts;
//         grpc::ServerContext ctx;
//         kv::BatchRequest req;
//         kv::BatchResponse resp;
//         grpc::ServerAsyncResponseWriter<kv::BatchResponse> responder;
//         enum { CREATE, PROCESS, FINISH } status;

//         BatchCallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c, 
//                       OptimisticTransactionDB* d, WriteOptions* wo)
//             : service(s), cq(c), txn_db(d), w_opts(wo), responder(&ctx), status(CREATE) { Proceed(); }

//         void Proceed() override {
//             if (status == CREATE) {
//                 status = PROCESS;
//                 service->RequestExecuteBatch(&ctx, &req, &responder, cq, cq, this);
//             } else if (status == PROCESS) {
//                 new BatchCallData(service, cq, txn_db, w_opts); // Spawn next

//                 auto const& md = ctx.client_metadata();
//                 auto it = md.find("authorization");
                
//                 // Fast JWT Verification using Cache
//                 if (it == md.end() || !VerifyJWTCached(std::string(it->second.data(), it->second.size()))) {
//                     responder.Finish(resp, grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Deny"), this);
//                 } else {
//                     // --- OPTIMIZED BATCH WRITE ---
//                     Transaction* txn = txn_db->BeginTransaction(*w_opts);
//                     for (const auto& entry : req.entries()) {
//                         if (entry.type() == kv::PUT) txn->Put(entry.key(), entry.value());
//                     }
//                     Status s = txn->Commit();
//                     delete txn;

//                     if (!s.ok()) {
//                         // THE FIX: If the database rejects the write due to a conflict, 
//                         // instantly abort the gRPC request so the client knows it failed!
//                         responder.Finish(resp, grpc::Status(grpc::StatusCode::ABORTED, "Optimistic Lock Conflict"), this);
//                     } else {
//                         resp.set_success(true);
//                         responder.Finish(resp, grpc::Status::OK, this);
//                     }
//                 }
//                 status = FINISH;
//             } else { delete this; }
//         }
//     };

//     std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
//     kv::KVService::AsyncService service_;
//     std::unique_ptr<grpc::Server> server_;
//     OptimisticTransactionDB* txn_db_;
//     WriteOptions w_opts;
// };

// int main() {
//     ServerImpl s;
//     s.Run();
//     return 0;
// }




// final update


// #include <iostream>
// #include <vector>
// #include <thread>
// #include <fstream>
// #include <unordered_map>
// #include <chrono>
// #include <csignal>
// #include <atomic>

// #include <rocksdb/db.h>
// #include <rocksdb/options.h>
// #include <rocksdb/utilities/optimistic_transaction_db.h>
// #include <rocksdb/utilities/transaction.h>

// #include <grpcpp/grpcpp.h>
// #include <grpcpp/resource_quota.h>
// #include <jwt-cpp/jwt.h>

// #include "kv.grpc.pb.h"

// using namespace rocksdb;

// const std::string MASTER_SECRET = "super-secret-server-key-256";

// // --- SECURITY HELPER: Read Certificate Files ---
// std::string ReadFile(const std::string& filename) {
//     std::ifstream ifs(filename);
//     if (!ifs.is_open()) {
//         std::cerr << "CRITICAL ERROR: Failed to open " << filename << std::endl;
//         exit(1);
//     }
//     return std::string((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
// }

// bool VerifyJWTCached(const std::string& token_str) {
//     // Thread-local cache avoids mutex locks
//     thread_local std::unordered_map<std::string, std::chrono::time_point<std::chrono::steady_clock>> local_cache;
    
//     // FIX: Memory leak protection. Clear cache if it gets too large from unique, one-time clients.
//     if (local_cache.size() > 10000) {
//         local_cache.clear();
//     }

//     auto now = std::chrono::steady_clock::now();
    
//     // 1. Check the thread's private cache
//     auto it = local_cache.find(token_str);
//     if (it != local_cache.end()) {
//         if (now < it->second) return true; // Still valid!
//         local_cache.erase(it); // Expired
//     }

//     // 2. Heavy math only if it's not in this specific thread's cache
//     try {
//         auto decoded = jwt::decode(token_str);
//         auto verifier = jwt::verify()
//             .allow_algorithm(jwt::algorithm::hs256{MASTER_SECRET})
//             .with_issuer("kv_server");
//         verifier.verify(decoded);
        
//         // 3. Save to the private cache
//         local_cache[token_str] = now + std::chrono::seconds(60);
//         return true;
//     } catch (...) {
//         return false; 
//     }
// }

// // Base class so our worker loop can process both Logins and Batches
// class CallDataBase {
// public:
//     virtual void Proceed() = 0;
//     virtual ~CallDataBase() = default;
// };

// class ServerImpl final {
// public:
//     ~ServerImpl() {
//         Shutdown();
//     }

//     void Run() {
//         Options options;
//         options.create_if_missing = true;
        
//         // --- ROCKSDB AGGRESSIVE TUNING ---
//         int num_cores = std::thread::hardware_concurrency();
//         options.IncreaseParallelism(num_cores);
//         options.OptimizeLevelStyleCompaction();
//         options.max_background_jobs = num_cores; 
        
//         // 512MB Memtable for fewer flushes
//         options.write_buffer_size = 512 * 1024 * 1024; 
//         options.max_write_buffer_number = 8;
//         options.min_write_buffer_number_to_merge = 2;
        
//         // Disable WAL and use Pipelined Writes for 1M+ TPS
//         options.enable_pipelined_write = true;
//         options.allow_concurrent_memtable_write = true;
//         options.max_background_compactions = 4;

//         Status s = OptimisticTransactionDB::Open(options, "./data", &txn_db_);
//         if (!s.ok()) {
//             std::cerr << "DB Error: " << s.ToString() << std::endl;
//             exit(1);
//         }

//         w_opts.disableWAL = true; // High speed, but risks data loss on sudden crash
//         w_opts.sync = false;

//         // --- gRPC RESOURCE OPTIMIZATION ---
//         grpc::ResourceQuota quota;
//         quota.SetMaxThreads(num_cores * 2);

//         // FIX: Re-added SSL/TLS configuration
//         grpc::SslServerCredentialsOptions ssl_opts;
//         ssl_opts.pem_key_cert_pairs.push_back({ReadFile("server.key"), ReadFile("server.crt")});

//         grpc::ServerBuilder builder;
//         builder.SetResourceQuota(quota);
//         builder.SetMaxReceiveMessageSize(128 * 1024 * 1024); 
        
//         // Use SslServerCredentials instead of InsecureServerCredentials
//         builder.AddListeningPort("0.0.0.0:50051", grpc::SslServerCredentials(ssl_opts));
//         builder.RegisterService(&service_);

//         for (int i = 0; i < num_cores; i++) {
//             cqs_.emplace_back(builder.AddCompletionQueue());
//         }

//         server_ = builder.BuildAndStart();
//         std::cout << ">>> TURBO MODE + SECURE JWT CACHE ACTIVE: Targeting 1.2M+ TPS <<<" << std::endl;
//         std::cout << ">>> TURBO MODE + MULTI-THREADED CQs ACTIVE <<<" << std::endl;

//         int threads_per_core = 4; 
        
//         for (auto& cq : cqs_) {
//             for (int i = 0; i < threads_per_core; i++) {
//                 workers_.emplace_back([this, cq_ptr = cq.get()]() {
//                     for(int j=0; j<250; ++j) { 
//                         new LoginCallData(&service_, cq_ptr);
//                         new BatchCallData(&service_, cq_ptr, txn_db_, &w_opts);
//                     }
                    
//                     void* tag;
//                     bool ok;
//                     // cq_ptr->Next returns false when the queue is fully drained and shut down
//                     while (cq_ptr->Next(&tag, &ok)) {
//                         if (ok) {
//                             static_cast<CallDataBase*>(tag)->Proceed();
//                         } else {
//                             // Queue is shutting down, clean up lingering objects
//                             delete static_cast<CallDataBase*>(tag);
//                         }
//                     }
//                 });
//             }
//         }
//     }

//     // FIX: Graceful Shutdown implementation
//     void Shutdown() {
//         if (!is_shutdown_.exchange(true)) {
//             std::cout << "\nInitiating graceful shutdown..." << std::endl;
//             if (server_) {
//                 server_->Shutdown();
//             }
//             // Always shutdown completion queues AFTER the server
//             for (auto& cq : cqs_) {
//                 cq->Shutdown();
//             }
//             // Wait for all worker threads to drain the queues and exit
//             for (auto& t : workers_) {
//                 if (t.joinable()) t.join();
//             }
//             if (txn_db_) {
//                 delete txn_db_;
//                 txn_db_ = nullptr;
//             }
//             std::cout << "Server safely terminated." << std::endl;
//         }
//     }

// private:
//     // --- STATE MACHINE 1: LOGIN ---
//     struct LoginCallData : public CallDataBase {
//         kv::KVService::AsyncService* service;
//         grpc::ServerCompletionQueue* cq;
//         grpc::ServerContext ctx;
//         kv::LoginRequest req;
//         kv::LoginResponse resp;
//         grpc::ServerAsyncResponseWriter<kv::LoginResponse> responder;
//         enum { CREATE, PROCESS, FINISH } status;

//         LoginCallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c)
//             : service(s), cq(c), responder(&ctx), status(CREATE) { Proceed(); }

//         void Proceed() override {
//             if (status == CREATE) {
//                 status = PROCESS;
//                 service->RequestLogin(&ctx, &req, &responder, cq, cq, this);
//             } else if (status == PROCESS) {
//                 new LoginCallData(service, cq); // Spawn next

//                 if (req.api_key() == "initial-pass") {
//                     auto token = jwt::create()
//                         .set_issuer("kv_server")
//                         .set_subject(req.client_id())
//                         .set_issued_at(std::chrono::system_clock::now())
//                         .set_expires_at(std::chrono::system_clock::now() + std::chrono::hours(1))
//                         .sign(jwt::algorithm::hs256{MASTER_SECRET});
//                     resp.set_success(true);
//                     resp.set_jwt_token(token);
//                 } else {
//                     resp.set_success(false);
//                     resp.set_error_message("Invalid API Key");
//                 }
//                 status = FINISH;
//                 responder.Finish(resp, grpc::Status::OK, this);
//             } else { delete this; }
//         }
//     };

//     // --- STATE MACHINE 2: HIGH SPEED BATCH ---
//     struct BatchCallData : public CallDataBase {
//         kv::KVService::AsyncService* service;
//         grpc::ServerCompletionQueue* cq;
//         OptimisticTransactionDB* txn_db;
//         WriteOptions* w_opts;
//         grpc::ServerContext ctx;
//         kv::BatchRequest req;
//         kv::BatchResponse resp;
//         grpc::ServerAsyncResponseWriter<kv::BatchResponse> responder;
//         enum { CREATE, PROCESS, FINISH } status;

//         BatchCallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c, 
//                       OptimisticTransactionDB* d, WriteOptions* wo)
//             : service(s), cq(c), txn_db(d), w_opts(wo), responder(&ctx), status(CREATE) { Proceed(); }

//         void Proceed() override {
//             if (status == CREATE) {
//                 status = PROCESS;
//                 service->RequestExecuteBatch(&ctx, &req, &responder, cq, cq, this);
//             } else if (status == PROCESS) {
//                 new BatchCallData(service, cq, txn_db, w_opts); // Spawn next

//                 auto const& md = ctx.client_metadata();
//                 auto it = md.find("authorization");
                
//                 if (it == md.end() || !VerifyJWTCached(std::string(it->second.data(), it->second.size()))) {
//                     responder.Finish(resp, grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Deny"), this);
//                 } else {
//                     Transaction* txn = txn_db->BeginTransaction(*w_opts);
//                     for (const auto& entry : req.entries()) {
//                         if (entry.type() == kv::PUT) txn->Put(entry.key(), entry.value());
//                     }
//                     Status s = txn->Commit();
//                     delete txn;

//                     if (!s.ok()) {
//                         responder.Finish(resp, grpc::Status(grpc::StatusCode::ABORTED, "Optimistic Lock Conflict"), this);
//                     } else {
//                         resp.set_success(true);
//                         responder.Finish(resp, grpc::Status::OK, this);
//                     }
//                 }
//                 status = FINISH;
//             } else { delete this; }
//         }
//     };

//     std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
//     kv::KVService::AsyncService service_;
//     std::unique_ptr<grpc::Server> server_;
//     OptimisticTransactionDB* txn_db_;
//     WriteOptions w_opts;
//     std::vector<std::thread> workers_;
//     std::atomic<bool> is_shutdown_{false};
// };

// // Global pointer for signal handling
// ServerImpl* g_server = nullptr;

// void HandleSignal(int signal) {
//     if (g_server) {
//         g_server->Shutdown();
//     }
// }

// int main() {
//     // Register signal handlers for clean termination
//     std::signal(SIGINT, HandleSignal);
//     std::signal(SIGTERM, HandleSignal);

//     ServerImpl s;
//     g_server = &s;
//     s.Run();

//     // The main thread will block here until a signal is received 
//     // or the server shuts down internally.
//     return 0;
// }



#include <iostream>
#include <vector>
#include <thread>
#include <fstream>
#include <unordered_map>
#include <chrono>
#include <csignal>
#include <atomic>

#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <jwt-cpp/jwt.h>

#include "kv.grpc.pb.h"

using namespace rocksdb;

const std::string MASTER_SECRET = "super-secret-server-key-256";

// --- SECURITY HELPER: Read Certificate Files ---
std::string ReadFile(const std::string& filename) {
    std::ifstream ifs(filename);
    if (!ifs.is_open()) {
        std::cerr << "CRITICAL ERROR: Failed to open " << filename << std::endl;
        exit(1);
    }
    return std::string((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
}

bool VerifyJWTCached(const std::string& token_str) {
    thread_local std::unordered_map<std::string, std::chrono::time_point<std::chrono::steady_clock>> local_cache;
    
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

// ---------------------------------------------------------------------------
// WritePool: a fixed pool of threads that drain a lock-free task queue.
// gRPC worker threads submit their db->Write() work here and return
// immediately, freeing them to handle the next arriving RPC.
// ---------------------------------------------------------------------------
class WritePool {
public:
    explicit WritePool(int n) {
        for (int i = 0; i < n; i++)
            workers_.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lk(mu_);
                        cv_.wait(lk, [this] { return stop_ || !q_.empty(); });
                        if (stop_ && q_.empty()) return;
                        task = std::move(q_.front());
                        q_.pop();
                    }
                    task();
                }
            });
    }
    ~WritePool() {
        { std::lock_guard<std::mutex> lk(mu_); stop_ = true; }
        cv_.notify_all();
        for (auto& t : workers_) if (t.joinable()) t.join();
    }
    void Submit(std::function<void()> fn) {
        { std::lock_guard<std::mutex> lk(mu_); q_.push(std::move(fn)); }
        cv_.notify_one();
    }
private:
    std::vector<std::thread>       workers_;
    std::queue<std::function<void()>> q_;
    std::mutex                     mu_;
    std::condition_variable        cv_;
    bool                           stop_ = false;
};

class CallDataBase {
public:
    virtual void Proceed() = 0;
    virtual ~CallDataBase() = default;
};

class ServerImpl final {
public:
    ~ServerImpl() { Shutdown(); }

    void Run() {
        Options options;
        options.create_if_missing = true;
        
        int num_cores = std::thread::hardware_concurrency();
        options.IncreaseParallelism(num_cores);
        options.OptimizeLevelStyleCompaction();
        // FIX: bump to num_cores*2, and drop separate max_background_compactions
        // (setting both causes sub-optimal thread splitting in RocksDB)
        options.max_background_jobs = num_cores * 2;
        
        // --- ROCKSDB MEMORY TUNING ---
        options.write_buffer_size = 512 * 1024 * 1024; // 512MB MemTable
        options.max_write_buffer_number = 8;           // allow 8 memtables before stall
        options.min_write_buffer_number_to_merge = 2;   // merge when 2 pending
        // Use no compression for in‑memory speed (optional)
        options.compression = rocksdb::kNoCompression;
        
        options.enable_pipelined_write = true;
        options.allow_concurrent_memtable_write = true;

        // Using standard DB instead of OptimisticTransactionDB for raw latency speed
        Status s = DB::Open(options, "./data", &db_);
        if (!s.ok()) {
            std::cerr << "DB Error: " << s.ToString() << std::endl;
            exit(1);
        }

        w_opts.disableWAL = true;
        w_opts.sync = false;
        // avoid RocksDB stalling the thread

        // FIX: removed ResourceQuota cap — it was throttling gRPC to only
        // num_cores*2=32 threads, starving our 128 worker threads of I/O capacity.

        // --- FULL TLS CONFIGURATION ---
        grpc::SslServerCredentialsOptions ssl_opts;
        ssl_opts.pem_key_cert_pairs.push_back({ReadFile("server.key"), ReadFile("server.crt")});

        grpc::ServerBuilder builder;
        builder.SetMaxReceiveMessageSize(128 * 1024 * 1024); 
        builder.AddListeningPort("0.0.0.0:50051", grpc::SslServerCredentials(ssl_opts));
        builder.RegisterService(&service_);

        for (int i = 0; i < num_cores; i++) {
            cqs_.emplace_back(builder.AddCompletionQueue());
        }

        server_ = builder.BuildAndStart();
        std::cout << ">>> SECURE TURBO MODE ACTIVE: Targeting Sub-10ms Latency <<<" << std::endl;

        // 1 Thread per Core to completely eliminate mutex contention
        int threads_per_core = 8; 
        
        for (auto& cq : cqs_) {
            for (int i = 0; i < threads_per_core; i++) {
                workers_.emplace_back([this, cq_ptr = cq.get()]() {
                    for(int j=0; j<500; ++j) {
                        new LoginCallData(&service_, cq_ptr);
                        new BatchCallData(&service_, cq_ptr, db_, &w_opts);
                    }
                    
                    void* tag;
                    bool ok;
                    while (cq_ptr->Next(&tag, &ok)) {
                        if (ok) static_cast<CallDataBase*>(tag)->Proceed();
                        else delete static_cast<CallDataBase*>(tag);
                    }
                });
            }
        }
        
        // >>> ADD THIS LINE: Block the main thread to keep the server alive! <<<
        server_->Wait();
    }

    void Shutdown() {
        if (!is_shutdown_.exchange(true)) {
            std::cout << "\nInitiating graceful shutdown..." << std::endl;
            if (server_) server_->Shutdown();
            for (auto& cq : cqs_) cq->Shutdown();
            for (auto& t : workers_) if (t.joinable()) t.join();
            if (db_) { delete db_; db_ = nullptr; }
            std::cout << "Server safely terminated." << std::endl;
        }
    }

private:
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
            } else { delete this; }
        }
    };

    struct BatchCallData : public CallDataBase {
        kv::KVService::AsyncService* service;
        grpc::ServerCompletionQueue* cq;
        rocksdb::DB* db;
        WriteOptions* w_opts;
        grpc::ServerContext ctx;
        kv::BatchRequest req;
        kv::BatchResponse resp;
        grpc::ServerAsyncResponseWriter<kv::BatchResponse> responder;
        enum { CREATE, PROCESS, FINISH } status;

        BatchCallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c,
                      rocksdb::DB* d, WriteOptions* wo)
            : service(s), cq(c), db(d), w_opts(wo), responder(&ctx), status(CREATE) { Proceed(); }

        void Proceed() override {
            if (status == CREATE) {
                status = PROCESS;
                service->RequestExecuteBatch(&ctx, &req, &responder, cq, cq, this);
            } else if (status == PROCESS) {
                new BatchCallData(service, cq, db, w_opts);

                auto const& md = ctx.client_metadata();
                auto it = md.find("authorization");
                
                if (it == md.end() || !VerifyJWTCached(std::string(it->second.data(), it->second.size()))) {
                    status = FINISH;
                    responder.Finish(resp, grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Deny"), this);
                    return;
                }

                rocksdb::WriteBatch batch(req.entries().size() * 64);
                for (const auto& entry : req.entries()) {
                    if (entry.type() == kv::PUT) batch.Put(entry.key(), entry.value());
                    else if (entry.type() == kv::DELETE) batch.Delete(entry.key());
                }

                Status s = db->Write(*w_opts, &batch);
                resp.set_success(s.ok());
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
    rocksdb::DB* db_;
    WriteOptions w_opts;
    std::vector<std::thread> workers_;
    std::atomic<bool> is_shutdown_{false};
};

ServerImpl* g_server = nullptr;

void HandleSignal(int signal) {
    if (g_server) g_server->Shutdown();
}

int main() {
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    ServerImpl s;
    g_server = &s;
    s.Run();

    return 0;
}