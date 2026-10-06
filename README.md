# Zeymir In-Memory Database Engine

[English](#english) | [Türkçe](#türkçe)

---

<a name="english"></a>
## 🚀 English Description

A high-performance, multi-threaded in-memory database engine built entirely from scratch in C++17. This project demonstrates core computer science concepts such as concurrency control, data persistence, and memory optimization without relying on external frameworks.

### Core Architecture
* **Multi-threading & Concurrency:** Utilizes `std::shared_mutex` to allow concurrent reads while safely locking write operations, entirely preventing race conditions during high-load client requests.
* **Write-Ahead Logging (WAL):** Ensures data durability. Every transaction is logged to a `zeymir_engine.wal` file before being committed to RAM, allowing full state recovery in the event of a system crash.
* **LRU Cache Eviction:** Implements a custom Least Recently Used (LRU) caching mechanism using `std::list` and `std::unordered_map` to manage memory footprint dynamically.
* **ACID-like Transactions:** Supports strict transaction blocks (`Begin`, `Commit`, `Rollback`). Failed operations are safely rolled back without corrupting the database state.

### Build and Run
This engine requires a compiler with C++17 support (e.g., GCC/MinGW). 

Compile the source code:
```bash
g++ -std=c++17 zeymirdg.cpp -o zeymirdg.exe -pthread
