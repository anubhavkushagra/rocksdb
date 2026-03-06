#include <iostream>
#include <thread>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction.h>
#include <grpcpp/grpcpp.h>
#include "kv.grpc.pb.h"

using namespace rocksdb;

class StorageImpl final : public kv::KVService::Service {
    OptimisticTransactionDB* txn_db_;
    WriteOptions w_opts;

public:
    StorageImpl() {
        Options options;
        options.create_if_missing = true;
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        options.write_buffer_size = 256 * 1024 * 1024;
        options.max_write_buffer_number = 4;
        options.enable_pipelined_write = true;
        options.allow_concurrent_memtable_write = true;

        Status s = OptimisticTransactionDB::Open(options, "../data", &txn_db_);
        if (!s.ok()) {
            std::cerr << "Failed to open RocksDB: " << s.ToString() << std::endl;
            exit(1);
        }
        w_opts.disableWAL = true;
        w_opts.sync = false;
    }

    ~StorageImpl() { delete txn_db_; }

    grpc::Status Login(grpc::ServerContext*, const kv::LoginRequest*,
                       kv::LoginResponse* resp) override {
        std::cout << "[i5 - RocksDB] Received Login request!" << std::endl;
        resp->set_success(true);
        return grpc::Status::OK;
    }

    grpc::Status ExecuteBatch(grpc::ServerContext*,
                              const kv::BatchRequest* req,
                              kv::BatchResponse* resp) override {
        static std::atomic<long> req_count{0};
        long cnt = ++req_count;
        
        // Log EVERY SINGLE request so we know for sure traffic is hitting RocksDB
        std::cout << "[i5 - RocksDB] Received ExecuteBatch #" << cnt 
                  << " | total keys=" << req->entries_size() << std::endl;

        OptimisticTransactionOptions txn_options;
        txn_options.set_snapshot = true;
        Transaction* txn = txn_db_->BeginTransaction(w_opts, txn_options);
        ReadOptions read_opts;

        for (const auto& entry : req->entries()) {
            if (entry.type() == kv::PUT) {
                txn->Put(entry.key(), entry.value());
            } else if (entry.type() == kv::DELETE) {
                txn->Delete(entry.key());
            } else if (entry.type() == kv::GET) {
                std::string val;
                txn->GetForUpdate(read_opts, entry.key(), &val);
                resp->add_values(val);
            }
        }

        Status s = txn->Commit();
        resp->set_success(s.ok());
        delete txn;

        if (s.ok()) return grpc::Status::OK;
        return grpc::Status(grpc::StatusCode::ABORTED, "RocksDB Conflict");
    }
};

int main() {
    StorageImpl service;
    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(128 * 1024 * 1024);
    builder.SetMaxSendMessageSize(128 * 1024 * 1024);
    builder.AddListeningPort("0.0.0.0:50005", grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    std::cout << ">>> INTERNAL STORAGE NODE (RocksDB) Live on 0.0.0.0:50005 <<<" << std::endl;
    std::cout << "[i5] Listening on port 50005. Waiting for requests..." << std::endl;
    server->Wait();
    return 0;
}