#include "Defs.hpp"
#include "PerfEvent.hpp"
#include "farm/Farm.hpp"
#include "farm/Config.hpp"
#include "farm/threads/Concurrency.hpp"
#include "farm/profiling/counters/WorkerCounters.hpp"
#include "farm/profiling/ProfilingThread.hpp"
#include "farm/utils/RandomGenerator.hpp"
#include "farm/utils/Time.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <iostream>
#include <chrono>
#include <fstream>
#include <random>
// -------------------------------------------------------------------------------------


template <u64 size>
struct BytesPayload {
   uint8_t value[size];
   BytesPayload() = default;
   bool operator==(BytesPayload& other) { return (std::memcmp(value, other.value, sizeof(value)) == 0); }
   bool operator!=(BytesPayload& other) { return !(operator==(other)); }
};

struct ycsb_t {
   static constexpr int id = 0;
   uint64_t key;
   uint64_t counter = 0;
   BytesPayload<128> value;

   std::string toString() { return std::to_string(key) + " "+ std::to_string(counter) + " "+ std::string((char*)value.value); };
};

// -------------------------------------------------------------------------------------
struct Partition {
   uint64_t begin;
   uint64_t end;
};
// -------------------------------------------------------------------------------------
int main(int argc, char *argv[])
{
   using namespace farm;
   gflags::SetUsageMessage("FaRM Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   FaRM db;
   db.registerTable<ycsb_t>("ycsb",0,100);
   db.startAndConnect();

   [[maybe_unused]] auto& table = db.getTable("ycsb");

   table.bulk_load_local<ycsb_t>([](uint64_t t_id, ycsb_t& record) {
      record.key = t_id;
      record.counter = 0;
      farm::utils::RandomGenerator::getRandString(reinterpret_cast<uint8_t*>(&record.value), sizeof(ycsb_t::value));
      std::cout << "original " << record.toString() << std::endl;
   });
   table.printTable<ycsb_t>();

   db.getWorkerPool().scheduleJobSync(0, [&]() {
      for(uint64_t t_id = 0; t_id < 100; t_id++){
         ycsb_t record;
         table.lookup<ycsb_t>(t_id, record);
         std::cout << "latch-free " << t_id << " " << record.toString() << std::endl;
      }
      for(uint64_t inc = 0; inc < 100; inc++){
         for(uint64_t t_id = 0; t_id < 100; t_id++){
            ycsb_t record;
            table.readModifyWrite<ycsb_t>(t_id, [](ycsb_t& record) { record.counter++; });
         }
      }
   });

   table.printTable<ycsb_t>();

   return 0;
}
