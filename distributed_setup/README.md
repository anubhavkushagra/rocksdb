# RocksDB 3-Tier Distributed Architecture

This repository contains the perfectly isolated codebases for running a massive 3-tier gRPC/RocksDB load test across three separate machines.

## Architecture

1. **`i5/` Folder (Storage Tier)**
   - Pure RocksDB backend database.
   - No TLS, No Auth overhead.
   - Optimized for extreme disk throughput.

2. **`amd/` Folder (Application Server / Proxy Tier)**
   - TLS/SSL termination & JWT Authentication.
   - Multi-threaded gRPC proxying to the storage tier.

3. **`i3/` Folder (Load Client Tier)**
   - High-throughput asynchronous load generator.
   - Requires public TLS certificate to authenticate with the Proxy Tier.
