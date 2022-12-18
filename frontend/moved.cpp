#include "Defs.hpp"
#include "PerfEvent.hpp"
#include "dtree/Compute.hpp"
#include "dtree/Config.hpp"
#include "dtree/Storage.hpp"
#include "dtree/profiling/ProfilingThread.hpp"
#include "dtree/profiling/counters/WorkerCounters.hpp"
#include "dtree/threads/Concurrency.hpp"
#include "dtree/threads/Worker.hpp"
#include "dtree/utils/RandomGenerator.hpp"
#include "dtree/utils/Time.hpp"
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
DEFINE_uint32(read_ratio, 100, "");
DEFINE_bool(scans, false, "use scans");
// selectivity values from paper  0.001 (0.1%), 0.01 (1%), 0.1 (10%)
DEFINE_double(scan_selectivity, 0.01, "scan selectivity");
DEFINE_uint32(run_for_seconds, 5, "");

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

//=== Profiling ===//
struct ProfilingInfo : public dtree::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t elements;
   uint64_t readRatio;
   std::string skew;
   uint64_t timestamp = 0;

   ProfilingInfo(std::string experiment, uint64_t elements, uint64_t readRatio, std::string skew)
       : experiment(experiment), elements(elements), readRatio(readRatio), skew(skew) {}

   virtual std::vector<std::string> getRow() {
      return {
          experiment, std::to_string(elements), std::to_string(readRatio), skew, std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader() { return {"workload", "elements", "read ratio", "skew", "timestamp"}; }

   virtual void csv(std::ofstream& file) override {
      file << experiment << " , ";
      file << elements << " , ";
      file << readRatio << " , ";
      file << skew << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override {
      file << "Workload"
           << " , ";
      file << "Elements"
           << " , ";
      file << "ReadRatio"
           << " , ";
      file << "Skew"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};

//=== Storage Logic ===//
void storage_node() {
   using namespace dtree;
   Storage store;
   profiling::EmptyWorkloadInfo wl;
   store.startProfiler(wl);
   store.startMessageHandler();
   {
      while (store.getConnectedClients() == 0)
         ;
      [[maybe_unused]] dtree::RemoteGuard rguard(store.getConnectedClients());
   }
   std::cout << "Stopped Profiler" << std::endl;
   store.stopProfiler();
}

//=== Main ===//
int main(int argc, char* argv[]) {
   using namespace dtree;
   gflags::SetUsageMessage("Dtree Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   //=== Node Partitions ===//
   std::string skew = (FLAGS_percentage_keys.empty()) ? "No skew" : FLAGS_percentage_keys;
   std::vector<std::pair<Key, Key>> partition_map;  // maps partitions to node_id [begin,end)
   auto get_partition = [&](Key& key) {
      for (uint64_t p_i = 0; p_i < partition_map.size(); p_i++)
         if (key >= partition_map[p_i].first && key < partition_map[p_i].second) return p_i;
      throw std::logic_error("Partition not found");
   };
   {
      std::vector<double> percentage_keys;
      // percentage has been supplied by the user other wise equi partition
      if (!FLAGS_percentage_keys.empty()) {
         auto tmp_pkeys = interpretGflagString(FLAGS_percentage_keys);
         for (auto pk : tmp_pkeys) { percentage_keys.push_back(pk / 100.0); }
      } else {
         percentage_keys.resize(FLAGS_storage_nodes);
         for (auto& pk : percentage_keys) { pk = 1.0 / (double)FLAGS_storage_nodes; }
      }
      for (uint64_t s_i = 0; s_i < FLAGS_storage_nodes; s_i++)
         partition_map.push_back(partition(s_i, percentage_keys, FLAGS_keys));
   }

   if (FLAGS_storage_node) {
      storage_node();
   } else {
      std::cout << "started compute node" << std::endl;
      Compute<threads::Worker> comp;
      comp.startAndConnect();
      //=== Barrier ===//
      uint64_t barrier_stage = 1;
      auto barrier_wait = [&]() {
         for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
            comp.getWorkerPool().scheduleJobAsync(
                t_i, [&, t_i]() { threads::Worker::my().rdma_barrier_wait(barrier_stage); });
         }
         comp.getWorkerPool().joinAll();
         barrier_stage++;
      };
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
               auto p_id = get_partition(k);
               Value v = k;
               threads::Worker::my().insert(p_id, k, v);
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            }
         });
      }

      barrier_wait();
      //=== Benchmark ===//
      std::string benchmark = (FLAGS_scans) ? "scans" : "point queries";
      ProfilingInfo pf{benchmark, FLAGS_keys, FLAGS_read_ratio, skew};
      comp.startProfiler(pf);
      std::atomic<bool> keep_running = true;
      std::atomic<u64> running_threads_counter = 0;
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         comp.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            running_threads_counter++;
            for (; keep_running; threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p)) {
               //=== Scan ===//
               if (FLAGS_scans) {
                  // pick partition and choose X values within
                  auto begin = utils::getTimePoint();
                  auto pp = partition_map[utils::RandomGenerator::getRandU64(0, partition_map.size())];  // pair
                  auto expected_values = static_cast<uint64_t>((double)(pp.second - pp.first) * FLAGS_scan_selectivity);
                  auto start = utils::RandomGenerator::getRandU64(pp.first, pp.second - expected_values);
                  auto kv_span = threads::Worker::my().scan(0, start, start + expected_values);
                  if (kv_span.empty()) throw std::logic_error("empty span");
                  for (const auto& kv : kv_span) {
                     if (kv.key != start)
                        throw std::logic_error("Key not as expected " + std::to_string(kv.key) + " vs " +
                                               std::to_string(start));
                     start++;
                  }
                  threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency,
                                                         utils::getTimePoint() - begin);
                  continue;
               }
               //=== Upsert and Lookups ===//
               auto begin = utils::getTimePoint();
               Key key = utils::RandomGenerator::getRandU64(0, FLAGS_keys);
               auto p_id = get_partition(key);
               if (FLAGS_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_read_ratio) {
                  Value rValue{0};
                  auto found = threads::Worker::my().lookup(p_id, key, rValue);
                  if (!found) throw std::logic_error("key not found");
               } else {
                  Value value = utils::RandomGenerator::getRandU64Fast();
                  auto success = threads::Worker::my().insert(p_id, key, value);
                  if (!success) throw std::logic_error("key not found");
               }
               threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency,
                                                      utils::getTimePoint() - begin);
            }
            running_threads_counter--;
         });
      }
      sleep(FLAGS_run_for_seconds);
      keep_running = false;
      while (running_threads_counter) _mm_pause();
      comp.getWorkerPool().joinAll();
      comp.stopProfiler();
   }
   return 0;
}
