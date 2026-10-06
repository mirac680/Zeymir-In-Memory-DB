#include <iostream>
#include <string>
#include <unordered_map>
#include <list>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <fstream>
#include <optional>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <thread>
#include <stdexcept>

namespace ZeymirCore {

struct Record {
    std::string value;
    uint64_t timestamp;
    uint32_t version;
};

class WriteAheadLog {
    std::ofstream fileStream;
    std::mutex fileMutex;
public:
    WriteAheadLog(const std::string& path) {
        fileStream.open(path, std::ios::app | std::ios::binary);
        if (!fileStream.is_open()) {
            throw std::runtime_error("WAL initialization failed");
        }
    }
    ~WriteAheadLog() {
        if (fileStream.is_open()) fileStream.close();
    }
    void writeEntry(const std::string& type, const std::string& key, const std::string& val) {
        std::lock_guard<std::mutex> lock(fileMutex);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        fileStream << ms << "\x01" << type << "\x01" << key << "\x01" << val << "\x02";
        fileStream.flush();
    }
};

template<typename K, typename V>
class EvictionCache {
    size_t maxSize;
    std::list<std::pair<K, V>> lruList;
    std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator> mapper;
    mutable std::shared_mutex syncMutex;

public:
    explicit EvictionCache(size_t limit) : maxSize(limit) {}

    void insert(const K& key, const V& value) {
        std::unique_lock<std::shared_mutex> lock(syncMutex);
        auto it = mapper.find(key);
        if (it != mapper.end()) {
            lruList.erase(it->second);
            mapper.erase(it);
        }
        lruList.push_front({key, value});
        mapper[key] = lruList.begin();

        if (mapper.size() > maxSize) {
            auto last = lruList.back();
            mapper.erase(last.first);
            lruList.pop_back();
        }
    }

    std::optional<V> retrieve(const K& key) {
        std::unique_lock<std::shared_mutex> lock(syncMutex);
        auto it = mapper.find(key);
        if (it == mapper.end()) return std::nullopt;
        lruList.splice(lruList.begin(), lruList, it->second);
        return it->second->second;
    }

    bool erase(const K& key) {
        std::unique_lock<std::shared_mutex> lock(syncMutex);
        auto it = mapper.find(key);
        if (it != mapper.end()) {
            lruList.erase(it->second);
            mapper.erase(it);
            return true;
        }
        return false;
    }

    std::vector<std::pair<K, V>> exportData() const {
        std::shared_lock<std::shared_mutex> lock(syncMutex);
        return {lruList.begin(), lruList.end()};
    }
};

class Transaction {
    std::unordered_map<std::string, std::optional<std::string>> modifications;
    bool active = false;
    std::string id;

public:
    Transaction() {
        auto now = std::chrono::system_clock::now().time_since_epoch().count();
        id = "TXN_" + std::to_string(now);
    }
    void begin() { active = true; modifications.clear(); }
    void put(const std::string& k, const std::string& v) { if(active) modifications[k] = v; }
    void remove(const std::string& k) { if(active) modifications[k] = std::nullopt; }
    bool isActive() const { return active; }
    void end() { active = false; }
    const auto& getModifications() const { return modifications; }
    const std::string& getId() const { return id; }
};

class DatabaseEngine {
    EvictionCache<std::string, Record> memoryStore;
    WriteAheadLog wal;
    std::mutex transactionMutex;
    std::unordered_map<std::thread::id, Transaction> activeTransactions;

    uint64_t getCurrentTime() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

public:
    DatabaseEngine(size_t capacity, const std::string& logDest)
        : memoryStore(capacity), wal(logDest) {}

    void executeDirectSet(const std::string& key, const std::string& value) {
        wal.writeEntry("PUT", key, value);
        Record rec{value, getCurrentTime(), 1};
        auto existing = memoryStore.retrieve(key);
        if (existing) rec.version = existing->version + 1;
        memoryStore.insert(key, rec);
    }

    std::optional<std::string> executeDirectGet(const std::string& key) {
        auto rec = memoryStore.retrieve(key);
        return rec ? std::make_optional(rec->value) : std::nullopt;
    }

    bool executeDirectDelete(const std::string& key) {
        wal.writeEntry("DEL", key, "");
        return memoryStore.erase(key);
    }

    void beginTransaction() {
        std::lock_guard<std::mutex> lock(transactionMutex);
        activeTransactions[std::this_thread::get_id()].begin();
        wal.writeEntry("TXN_BEGIN", activeTransactions[std::this_thread::get_id()].getId(), "");
    }

    void commitTransaction() {
        std::lock_guard<std::mutex> lock(transactionMutex);
        auto tid = std::this_thread::get_id();
        if (activeTransactions.find(tid) != activeTransactions.end() && activeTransactions[tid].isActive()) {
            auto txnId = activeTransactions[tid].getId();
            wal.writeEntry("TXN_PREPARE", txnId, "");
            for (const auto& mod : activeTransactions[tid].getModifications()) {
                if (mod.second) {
                    executeDirectSet(mod.first, mod.second.value());
                } else {
                    executeDirectDelete(mod.first);
                }
            }
            wal.writeEntry("TXN_COMMIT", txnId, "");
            activeTransactions[tid].end();
            activeTransactions.erase(tid);
        }
    }

    void rollbackTransaction() {
        std::lock_guard<std::mutex> lock(transactionMutex);
        auto tid = std::this_thread::get_id();
        if (activeTransactions.find(tid) != activeTransactions.end()) {
            wal.writeEntry("TXN_ROLLBACK", activeTransactions[tid].getId(), "");
            activeTransactions[tid].end();
            activeTransactions.erase(tid);
        }
    }

    void transactionalSet(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(transactionMutex);
        auto tid = std::this_thread::get_id();
        if (activeTransactions.find(tid) != activeTransactions.end() && activeTransactions[tid].isActive()) {
            activeTransactions[tid].put(key, value);
        } else {
            executeDirectSet(key, value);
        }
    }

    void generateSnapshot(const std::string& filename) {
        std::ofstream out(filename, std::ios::binary);
        auto snapshotData = memoryStore.exportData();
        for (const auto& row : snapshotData) {
            out << row.first << "\x00" << row.second.value << "\x00"
                << row.second.timestamp << "\x00" << row.second.version << "\n";
        }
    }
};

}

void simulateClientWorkload(ZeymirCore::DatabaseEngine& db, int clientId) {
    std::string prefix = "client_" + std::to_string(clientId) + "_";
    
    db.beginTransaction();
    db.transactionalSet(prefix + "session", "active");
    db.transactionalSet(prefix + "request_count", "0");
    db.commitTransaction();

    for (int i = 0; i < 50; ++i) {
        db.executeDirectSet(prefix + "log_" + std::to_string(i), "data_packet");
    }

    db.beginTransaction();
    db.transactionalSet(prefix + "status", "processing");
    db.rollbackTransaction(); 
}

int main() {
    try {
        ZeymirCore::DatabaseEngine engine(10000, "zeymir_engine.wal");

        engine.executeDirectSet("sys_boot_config", "x86_64_optimized");
        engine.executeDirectSet("max_memory_mb", "4096");

        std::vector<std::thread> clients;
        for (int i = 1; i <= 8; ++i) {
            clients.emplace_back(simulateClientWorkload, std::ref(engine), i);
        }

        for (auto& t : clients) {
            if (t.joinable()) t.join();
        }

        auto config = engine.executeDirectGet("sys_boot_config");
        if (config) {
            std::cout << "System configuration: " << config.value() << "\n";
        }

        engine.generateSnapshot("engine_state.rdb");
        std::cout << "Multi-threaded core engine simulation completed without errors.\n";

    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}