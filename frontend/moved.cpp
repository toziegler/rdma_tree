#include "Defs.hpp"
#include "PerfEvent.hpp"
#include "farm/Compute.hpp"
#include "farm/Config.hpp"
#include "farm/Storage.hpp"
#include "farm/profiling/ProfilingThread.hpp"
#include "farm/profiling/counters/WorkerCounters.hpp"
#include "farm/threads/Concurrency.hpp"
#include "farm/utils/RandomGenerator.hpp"
#include "farm/utils/Time.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <charconv>
#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
// -------------------------------------------------------------------------------------
DEFINE_string(percentage_keys, "",
              "percentage of keys per node, must be space delimited and integer scaled 100 -> 1.0");
DEFINE_uint64(keys, 100, "Number keys accross all nodes");
DEFINE_uint64(cid, 0, "Compute node id");

//=== Input parsing ===//
static std::vector<unsigned> interpretGflagString(std::string_view desc) {
   std::vector<unsigned> splitted;
   auto add = [&](std::string_view desc) {
      unsigned c = 0;
      std::from_chars(desc.data(), desc.data() + desc.length(), c);
      splitted.push_back(c);
   };
   while (desc.find(' ') != std::string_view::npos) {
      auto split = desc.find(' ');
      add(desc.substr(0, split));
      desc = desc.substr(split + 1);
   }
   add(desc);
   return splitted;
}

//=== Partitioning ===//
// [begin,end) without end and does not handle left overs
std::pair<Key, Key> partition(uint64_t id, std::vector<double> prob, uint64_t N) {
   uint64_t begin{0}, end{0};
   for (uint64_t i = 0; i <= id; ++i) {
      begin += end;
      end = static_cast<uint64_t>(static_cast<double>(N) * prob[i]);
   }
   return {begin, begin + end};
}
// equi partitioning
std::pair<Key, Key> equi_partition(uint64_t id, uint64_t participants, uint64_t N) {
   const uint64_t blockSize = N / participants;
   auto begin = id * blockSize;
   auto end = begin + blockSize;
   if (id == participants - 1) end = N;
   return {begin, end};
}

void storage_node(){
   using namespace farm;
   Storage store;
   store.startMessageHandler();
   sleep(5);
}

//=== Main ===//
int main(int argc, char* argv[]) {
   using namespace farm;
   gflags::SetUsageMessage("FaRM Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   //=== Node Partitions ===//
   std::vector<std::pair<Key,Key>> partition_map; // maps partitions to node_id [begin,end)
   {
      std::vector<double> percentage_keys;
      // percentage has been supplied by the user other wise equi partition
      if (!FLAGS_percentage_keys.empty()) {
         auto tmp_pkeys = interpretGflagString(FLAGS_percentage_keys);
         for (auto pk : tmp_pkeys) { percentage_keys.push_back(pk / 100.0); }
      }else{
         percentage_keys.resize(FLAGS_storage_nodes);
         for (auto& pk : percentage_keys) { pk = 100.0 / (double)FLAGS_storage_nodes; }
      }
      for (uint64_t s_i = 0; s_i < FLAGS_storage_nodes; s_i++)
         partition_map.push_back(partition(s_i, percentage_keys, FLAGS_keys));
   }
   
   if (FLAGS_storage_node) {
      storage_node();
   } else {
      std::cout << "started compute node" << std::endl;
      Compute comp;
      comp.startAndConnect();
      //=== build tree ===//
      // get compute node partition
      const auto part = equi_partition(FLAGS_cid, FLAGS_compute_nodes, FLAGS_keys);
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         comp.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            auto nodeKeys = part.second - part.first;
            auto threadPartition = equi_partition(t_i, FLAGS_worker, nodeKeys);
            auto begin = part.first + threadPartition.first;
            auto end = part.first + threadPartition.second;
            
            for (Key k = begin; k < end; ++k) {
               Value v = k;
               threads::Worker::my().insert(0, k, v);
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            }
         });
      }
      // XXX: Use and test barrier
      // XXX: Profiling
      // XXX: Benchmark time
      // XXX: Benchmark Workload 
      //=== Benchmark ===//
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         comp.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            auto s = threads::Worker::my().scan(0, 1, 99);
            for (auto& kv : s) { std::cout << "key " << kv.key << " " << kv.value << "\n"; }
         });
      }
      comp.getWorkerPool().joinAll();
      std::cout << "finishing"
                << "\n";
   }
   return 0;
}
