#include "PerfEvent.hpp"
#include "dtree/Config.hpp"
#include "dtree/Dtree.hpp"
#include "dtree/rdma/CommunicationManager.hpp"
#include "dtree/utils/RandomGenerator.hpp"
#include "dtree/utils/ScrambledZipfGenerator.hpp"
#include "dtree/utils/Time.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
DEFINE_uint32(YCSB_read_ratio, 100, "");
DEFINE_bool(YCSB_all_workloads, false , "Execute all workloads i.e. 50 95 100 ReadRatio on same tree");
DEFINE_uint64(YCSB_tuple_count, 1, " Tuple count in");
DEFINE_double(YCSB_zipf_factor, 0.0, "Default value according to spec");
DEFINE_double(YCSB_run_for_seconds, 10.0, "");
DEFINE_bool(YCSB_partitioned, false, "");
DEFINE_bool(YCSB_warm_up, false, "");
DEFINE_bool(YCSB_record_latency, false, "");
DEFINE_bool(YCSB_all_zipf, false, "");
DEFINE_bool(YCSB_local_zipf, false, "");
DEFINE_bool(YCSB_backoff, true, "");
// -------------------------------------------------------------------------------------
using u64 = uint64_t;
using u8 = uint8_t;
// -------------------------------------------------------------------------------------
template <u64 size>
struct BytesPayload {
   u8 value[size];
   BytesPayload() = default;
   bool operator==(BytesPayload& other) { return (std::memcmp(value, other.value, sizeof(value)) == 0); }
   bool operator!=(BytesPayload& other) { return !(operator==(other)); }
};
// -------------------------------------------------------------------------------------
struct Partition {
   uint64_t begin;
   uint64_t end;
};
// -------------------------------------------------------------------------------------
struct YCSB_workloadInfo : public dtree::profiling::WorkloadInfo {
 std::string experiment;
   uint64_t elements;
   uint64_t readRatio;
   double zipfFactor;
   std::string zipfOffset;
   uint64_t timestamp = 0;

   YCSB_workloadInfo(std::string experiment, uint64_t elements, uint64_t readRatio, double zipfFactor, std::string zipfOffset)
      : experiment(experiment), elements(elements), readRatio(readRatio), zipfFactor(zipfFactor), zipfOffset(zipfOffset)
   {
   }

   
   virtual std::vector<std::string> getRow(){
      return {
          experiment, std::to_string(elements),    std::to_string(readRatio), std::to_string(zipfFactor),
          zipfOffset, std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader(){
      return {"workload","elements","read ratio", "zipfFactor", "zipfOffset", "timestamp"};
   }
   

   virtual void csv(std::ofstream& file) override
   {
      file << experiment << " , ";
      file << elements << " , ";
      file << readRatio << " , ";
      file << zipfFactor << " , ";
      file << zipfOffset << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override
   {
      file << "Workload"
           << " , ";
      file << "Elements"
           << " , ";
      file << "ReadRatio"
           << " , ";
      file << "ZipfFactor"
           << " , ";
      file << "ZipfOffset"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};
// -------------------------------------------------------------------------------------
struct ycsb_t {
   static constexpr int id = 0;
   uint64_t key;
   BytesPayload<128> payload;

   std::string toString() { return std::to_string(key) + " "+ " "+ std::string((char*)payload.value); };
};

// -------------------------------------------------------------------------------------
int main(int argc, char* argv[])
{
   using namespace dtree;
   std::cout << "Dtree-like YCSB benchmark" << std::endl;
   // -------------------------------------------------------------------------------------
   gflags::SetUsageMessage("Catalog Test");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   // prepare workload
   std::vector<std::string> workload_type; // warm up or benchmark
   std::vector<uint32_t> workloads;
   std::vector<double> zipfs;
   if(FLAGS_YCSB_all_workloads){
      workloads.push_back(5); 
      workloads.push_back(50); 
      workloads.push_back(95);
      workloads.push_back(100);
   }else{
      workloads.push_back(FLAGS_YCSB_read_ratio);
   }
   
   if(FLAGS_YCSB_warm_up){
      workload_type.push_back("YCSB_warm_up");
      workload_type.push_back("YCSB_txn");
   }else{
      workload_type.push_back("YCSB_txn");
   }

   if(FLAGS_YCSB_all_zipf){
      zipfs.insert(zipfs.end(), {0,0.25,0.5,0.75,1.0,1.25,1.5,1.75,2.0});
   }else{
      zipfs.push_back(FLAGS_YCSB_zipf_factor);
   }
   // -------------------------------------------------------------------------------------
   Dtree db;
   // -------------------------------------------------------------------------------------
   auto partition = [&](uint64_t id, uint64_t participants, uint64_t N) -> Partition {
      const uint64_t blockSize = N / participants;
      auto begin = id * blockSize;
      auto end = begin + blockSize;
      if (id == participants - 1)
         end = N;
      return {.begin = begin, .end = end};
   };
   uint64_t barrier_stage = 1;
   auto barrier_wait = [&]() {
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() { threads::Worker::my().rdma_barrier_wait(barrier_stage); });
      }
      db.getWorkerPool().joinAll();
      barrier_stage++;
   }; 
   // -------------------------------------------------------------------------------------
   u64 YCSB_tuple_count = FLAGS_YCSB_tuple_count;
   auto nodePartition = partition(db.getNodeID(), FLAGS_nodes, YCSB_tuple_count);
   // -------------------------------------------------------------------------------------
   // create Table 
   // -------------------------------------------------------------------------------------
   db.registerTable<ycsb_t>("ycsb",nodePartition.begin,nodePartition.end);
   auto& table = db.getTable("ycsb");
   // -------------------------------------------------------------------------------------
   // start db
   db.startAndConnect();
   // -------------------------------------------------------------------------------------
   // Fill Table 
   // -------------------------------------------------------------------------------------
   std::cout << "Data generation " << std::endl;
   for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
      db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
         table.mt_bulk_load_local<ycsb_t>(t_i, [](uint64_t t_id, ycsb_t& record) {
            record.key = t_id;
            utils::RandomGenerator::getRandString(reinterpret_cast<uint8_t*>(&record.payload), sizeof(ycsb_t::payload));
         });
      });
   }
   db.getWorkerPool().joinAll();
   // -------------------------------------------------------------------------------------
   std::cout << "Data generation [OK]" << std::endl;
   // -------------------------------------------------------------------------------------
   for (auto ZIPF : zipfs) {
      // -------------------------------------------------------------------------------------
      // YCSB Transaction
      // -------------------------------------------------------------------------------------
      std::unique_ptr<utils::ScrambledZipfGenerator> zipf_random;
      if (FLAGS_YCSB_partitioned)
         zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(nodePartition.begin, nodePartition.end - 1,
                                                                       ZIPF);
      else
         zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, YCSB_tuple_count, ZIPF);

      // -------------------------------------------------------------------------------------
      // zipf creation can take some time due to floating point loop therefore wait with barrier
      barrier_wait();
      // -------------------------------------------------------------------------------------
      for (auto READ_RATIO : workloads) {
         for (auto TYPE : workload_type) {
            barrier_wait();
            std::atomic<bool> keep_running = true;
            std::atomic<u64> running_threads_counter = 0;

            uint64_t zipf_offset = 0;
            if (FLAGS_YCSB_local_zipf) zipf_offset = (YCSB_tuple_count / FLAGS_nodes) * db.getNodeID();

            YCSB_workloadInfo experimentInfo{TYPE, YCSB_tuple_count, READ_RATIO, ZIPF, (FLAGS_YCSB_local_zipf?"local_zipf":"global_zipf") };
            db.startProfiler(experimentInfo);
            for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
               db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
                  running_threads_counter++;
                  while (keep_running) {
                     uint64_t t_id = zipf_random->rand(zipf_offset);
                     ensure(t_id < YCSB_tuple_count);
                     ycsb_t record;

                     if (READ_RATIO == 100 || utils::RandomGenerator::getRandU64(0, 100) < READ_RATIO) {
                        auto start = utils::getTimePoint();
                        auto success = table.lookup(t_id, record);
                        ensure(success);
                        if(record.key != t_id){
                           GDB();
                        }
                        auto end = utils::getTimePoint();
                        threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                     } else {
                        record.key = t_id;
                        utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&record.payload), sizeof(ycsb_t::payload));
                        auto start = utils::getTimePoint();
                        table.readModifyWrite<ycsb_t>(t_id, [](ycsb_t& record) {
                           utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&record.payload), sizeof(ycsb_t::payload));
                        });
                        auto end = utils::getTimePoint();
                        threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                     }
                     threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
                  }
                  running_threads_counter--;
               });
            }
            // -------------------------------------------------------------------------------------
            // Join Threads
            // -------------------------------------------------------------------------------------
            sleep(FLAGS_YCSB_run_for_seconds);
            keep_running = false;
            while (running_threads_counter) {
               _mm_pause();
            }
            db.getWorkerPool().joinAll();
            // -------------------------------------------------------------------------------------
            db.stopProfiler();
            barrier_wait();
            if (FLAGS_YCSB_record_latency) {
               std::atomic<bool> keep_running = true;
               constexpr uint64_t LATENCY_SAMPLES = 1e6;
               YCSB_workloadInfo experimentInfo{"Latency", YCSB_tuple_count, READ_RATIO, ZIPF,
                                                (FLAGS_YCSB_local_zipf ? "local_zipf" : "global_zipf")};
               db.startProfiler(experimentInfo);
               std::vector<uint64_t> tl_microsecond_latencies[FLAGS_worker];
               std::vector<uint64_t> samples_taken(FLAGS_worker);

               for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
                   db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
                     running_threads_counter++;
                     uint64_t ops = 0;
                     tl_microsecond_latencies[t_i].reserve(LATENCY_SAMPLES);
                     while (keep_running) {
                        uint64_t t_id = zipf_random->rand(zipf_offset);
                        ensure(t_id < YCSB_tuple_count);
                        ycsb_t record;
                        if (READ_RATIO == 100 || utils::RandomGenerator::getRandU64(0, 100) < READ_RATIO) {
                           auto start = utils::getTimePoint();
                           auto success = table.lookup(t_id, record);
                           ensure(success);
                           ensure(record.key == t_id);
                           auto end = utils::getTimePoint();
                           threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                           if(ops < LATENCY_SAMPLES)
                              tl_microsecond_latencies[t_i].push_back(end - start);
                        } else {
                           record.key = t_id;
                           utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&record.payload), sizeof(ycsb_t::payload));
                           auto start = utils::getTimePoint();
                           table.readModifyWrite<ycsb_t>(t_id, [](ycsb_t& record) {
                              utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&record.payload), sizeof(ycsb_t::payload));
                           });
                           auto end = utils::getTimePoint();
                           threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                           if(ops < LATENCY_SAMPLES)
                              tl_microsecond_latencies[t_i].push_back(end - start);
                        }
                        threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
                        ops++;
                     }
                     samples_taken[t_i] = ops;
                     running_threads_counter--;
                  });
               }
               sleep(10);
               keep_running = false;
               while (running_threads_counter) {
                  _mm_pause();
               }
               // -------------------------------------------------------------------------------------
               // Join Threads
               // -------------------------------------------------------------------------------------
               db.getWorkerPool().joinAll();
               // -------------------------------------------------------------------------------------
               db.stopProfiler();
               // -------------------------------------------------------------------------------------
               // combine vector of threads into one
               std::vector<uint64_t> microsecond_latencies;
               for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
                  microsecond_latencies.insert(microsecond_latencies.end(), tl_microsecond_latencies[t_i].begin(), tl_microsecond_latencies[t_i].end());
               }
               
               {
                  std::cout << "Shuffle samples " << microsecond_latencies.size() << std::endl;
                  std::random_device rd;
                  std::mt19937 g(rd());
                  std::shuffle(microsecond_latencies.begin(), microsecond_latencies.end(), g);

                  // write out 400 samples
                  std::ofstream latency_file;
                  std::ofstream::openmode open_flags = std::ios::app;
                  std::string filename = "latency_samples_" + FLAGS_csvFile;
                  bool csv_initialized = std::filesystem::exists(filename);
                  latency_file.open(filename, open_flags);
                  if (!csv_initialized) {
                     latency_file << "workload,tag,ReadRatio,YCSB_tuple_count,zipf,latency" << std::endl;
                  }
                  for(uint64_t s_i =0; s_i < 1000; s_i++){
                     latency_file << TYPE << ","<<FLAGS_tag << "," << READ_RATIO << "," << YCSB_tuple_count << "," << ZIPF << "," << microsecond_latencies[s_i] << std::endl;
                  }
                  latency_file.close();
                  
               }
               std::cout << "Sorting Latencies"
                         << "\n";
               std::sort(microsecond_latencies.begin(), microsecond_latencies.end());
               std::cout << "Latency (min/median/max/99%): " << (microsecond_latencies[0]) << ","
                         << (microsecond_latencies[microsecond_latencies.size() / 2]) << ","
                         << (microsecond_latencies.back()) << ","
                         << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.99)]) << std::endl;
               // -------------------------------------------------------------------------------------
               // write to csv file
               std::ofstream latency_file;
               std::ofstream::openmode open_flags = std::ios::app;
               std::string filename = "latency_" + FLAGS_csvFile;
               bool csv_initialized = std::filesystem::exists(filename);
               latency_file.open(filename, open_flags);
               if (!csv_initialized) {
                  latency_file << "workload,tag,ReadRatio,YCSB_tuple_count,zipf,min,median,max,95th,99th,999th" << std::endl;
               }
               latency_file << TYPE << ","<<FLAGS_tag << ","  << READ_RATIO << "," << YCSB_tuple_count << "," << ZIPF << "," << (microsecond_latencies[0])
                            << "," << (microsecond_latencies[microsecond_latencies.size() / 2]) << ","
                            << (microsecond_latencies.back()) << ","
                            << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.95)]) << ","
                            << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.99)]) << ","
                            << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.999)])
                            << std::endl;
               latency_file.close();
            }
         }
      }
   }
   return 0;
}
