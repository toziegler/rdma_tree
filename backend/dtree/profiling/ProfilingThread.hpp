#pragma once
#include "CounterRegistry.hpp"
#include "counters/WorkerCounters.hpp"
#include "Tabulate.hpp"
#include "Defs.hpp"
#include "dtree/Config.hpp"

// -------------------------------------------------------------------------------------
#include <chrono>
#include <iostream>
#include <filesystem>
#include <thread>
// -------------------------------------------------------------------------------------
namespace dtree
{
namespace profiling
{

struct WorkloadInfo{
   virtual std::vector<std::string> getRow() = 0;
   virtual std::vector<std::string> getHeader() = 0;
   virtual void csv(std::ofstream& file) = 0;
   virtual void csvHeader(std::ofstream& file) = 0;
   virtual ~WorkloadInfo() = default;
};

struct EmptyWorkloadInfo : WorkloadInfo{
   virtual std::vector<std::string> getRow(){ return {};}
   virtual std::vector<std::string> getHeader(){ return {};}
   virtual void csv(std::ofstream& /*file*/){}
   virtual void csvHeader(std::ofstream& /*file*/){}
};


inline void wait_until_next(decltype(std::chrono::high_resolution_clock::now()) next) {
   using namespace std::chrono;
   high_resolution_clock::time_point current = high_resolution_clock::now();
   duration<double> time_span = duration_cast<duration<double>>(next - current);
   while (time_span.count() > 0) {
      current = high_resolution_clock::now();
      time_span = duration_cast<duration<double>>(next - current);
      _mm_pause();
   }
}

struct ProfilingThread {
   void profile(NodeID nodeId, WorkloadInfo& wlInfo)
   {
      using namespace std::chrono_literals;
      std::locale::global(std::locale("C")); // hack to restore locale which is messed up in tabulate package
      std::vector<uint64_t> workerCounterAgg(WorkerCounters::COUNT, 0);
      std::vector<double> rdmaCounterAgg(RDMACounters::COUNT, 0);
      std::unordered_map<std::string, double> cpuCountersAgg;
      // csv file
      std::ofstream csv_file;
      std::ofstream::openmode open_flags = std::ios::app;
      bool csv_initialized = std::filesystem::exists(FLAGS_csvFile);

      if (FLAGS_csv) {
         // create file and fill with headers
         csv_file.open(FLAGS_csvFile, open_flags);
      }

      auto convert_precision = [](double number) -> std::string {
         std::ios stream_state(nullptr);
         std::stringstream stream;
         stream_state.copyfmt(stream);
         stream << std::fixed << std::setprecision(2) << number;
         stream.copyfmt(stream_state);
         return stream.str();
      };

      auto convert_humanreadable = [&](double number)-> std::string {
         double hr_tx = 0;
         std::string unit = "";
         if (number >= 1e6) {
            hr_tx = number / 1000000.0;
            unit = "M";
         } else if (number >= 1e4) {
            hr_tx = number / 1000.0;
            unit = "K";
         }else{
            return convert_precision(number);
         }
         return convert_precision(hr_tx) + unit;
      };

      uint64_t seconds = 0;

      tabulate::Table::Row_t header;
      tabulate::Table::Row_t row;
      auto next = std::chrono::system_clock::now() + 1s;
      while (running) {
         seconds++;
         // -------------------------------------------------------------------------------------
         CounterRegistry::getInstance().aggregateWorkerCounters(workerCounterAgg);
         // -------------------------------------------------------------------------------------
         for (uint64_t c_i = 0; c_i < WorkerCounters::COUNT; c_i++) {
            if (WorkerCounters::workerCounterLogLevel[c_i].level > ACTIVE_LOG_LEVEL)
               continue;
            // -------------------------------------------------------------------------------------
            if(c_i == WorkerCounters::tx_p){
               header.push_back({WorkerCounters::workerCounterTranslation[c_i]});
               row.push_back(convert_humanreadable(static_cast<double>(workerCounterAgg[c_i])));
               continue;
            }
            // -------------------------------------------------------------------------------------
            if (c_i == WorkerCounters::latency && workerCounterAgg[WorkerCounters::tx_p] > 0) {
               header.push_back({WorkerCounters::workerCounterTranslation[c_i]});
               row.push_back(std::string(convert_precision(static_cast<double>(workerCounterAgg[c_i]) / (double)workerCounterAgg[WorkerCounters::tx_p])));
               continue;
            }
            // -------------------------------------------------------------------------------------
            header.push_back({WorkerCounters::workerCounterTranslation[c_i]});
            row.push_back(std::string(std::to_string(workerCounterAgg[c_i])));
         }
         // -------------------------------------------------------------------------------------
         auto tx_p = workerCounterAgg[WorkerCounters::tx_p];
         CounterRegistry::getInstance().aggregateCPUCounter(cpuCountersAgg);
         header.insert(header.end(), {{"inst/tx"}, {"L1-M/tx"}, {"cycl/tx"}, {"LLC-M/tx"}, {"CPU"}});
         row.insert(row.end(), { convert_humanreadable(cpuCountersAgg["instructions"]/static_cast<double>(tx_p)),
                                 convert_precision(cpuCountersAgg["L1-misses"] / static_cast<double>(tx_p)),
                                 convert_humanreadable(cpuCountersAgg["cycles"] / static_cast<double>(tx_p)),
                                 convert_precision(cpuCountersAgg["LLC-misses"] / static_cast<double>(tx_p)),
                                 convert_precision(cpuCountersAgg["CPU"])});

         // -------------------------------------------------------------------------------------
         // workload info
         auto wl_header = wlInfo.getHeader();
         header.insert(header.end(), wl_header.begin(), wl_header.end());
         auto wl_row = wlInfo.getRow();
         row.insert(row.end(), wl_row.begin(), wl_row.end());
         CounterRegistry::getInstance().aggregateRDMACounters(rdmaCounterAgg);
         for (uint64_t c_i = 0; c_i < RDMACounters::COUNT; c_i++) {
            header.push_back(RDMACounters::translation[c_i]);
            row.push_back(convert_precision(rdmaCounterAgg[c_i]));
         }
         // -------------------------------------------------------------------------------------
         // table
         {
            tabulate::Table table;
            table.format().width(10);
            if (seconds == 1) {
               table.add_row(header);
               table.add_row(row);
            } else {
               table.format().hide_border_top();
               table.add_row(row);
            }
            row.clear();
            header.clear();
            std::cout << table << std::endl;
         }
         // -------------------------------------------------------------------------------------
         // write to csv
         // -------------------------------------------------------------------------------------
         if (FLAGS_csv) {
            // called only once
            if (!csv_initialized) {
               // create columns
               for (uint64_t c_i = 0; c_i < WorkerCounters::COUNT; c_i++) {
                  csv_file << WorkerCounters::workerCounterTranslation[c_i] << " , ";
               }

               csv_file << "instructions/tx , ";
               csv_file << "L1-misses/tx , ";
               csv_file << "cycles/tx  , ";
               csv_file << "LLC-misses/tx , ";
               csv_file << "CPUs  , ";
               csv_file << "Workers ,";
               csv_file << "StorageNodes ,";
               csv_file << "ComputeNodes ,";
               csv_file << "NodeId ,";
               csv_file << "PPThreads ,";
               csv_file << "Cooling ,";
               csv_file << "EvictCoolestEpoch ,";
               csv_file << "Free ,";
               csv_file << "Tag ,";
               wlInfo.csvHeader(csv_file);

               // -------------------------------------------------------------------------------------

               for (uint64_t c_i = 0; c_i < RDMACounters::COUNT - 1; c_i++) {
                  csv_file << RDMACounters::translation[c_i] << " ,  ";
               }
               csv_file << RDMACounters::translation[RDMACounters::COUNT - 1] << std::endl;
               csv_initialized = true;
            }


            for (uint64_t c_i = 0; c_i < WorkerCounters::COUNT; c_i++) {
               if (c_i == WorkerCounters::latency && workerCounterAgg[WorkerCounters::tx_p] > 0) {
                  csv_file << static_cast<double>(workerCounterAgg[c_i]) / static_cast<double>(workerCounterAgg[WorkerCounters::tx_p]) << " , ";
               } else {
                  csv_file << workerCounterAgg[c_i] << " , ";
               }
            }

            csv_file << cpuCountersAgg["instructions"] / static_cast<double>(tx_p) << " , ";
            csv_file << cpuCountersAgg["L1-misses"] / static_cast<double>(tx_p) << " , ";
            csv_file << cpuCountersAgg["cycles"] / static_cast<double>(tx_p) << " , ";
            csv_file << cpuCountersAgg["LLC-misses"] / static_cast<double>(tx_p) << " , ";
            csv_file << cpuCountersAgg["CPU"] << " , ";
            csv_file << FLAGS_worker << " , ";
            csv_file << FLAGS_storage_nodes << " , ";
            csv_file << FLAGS_compute_nodes << " , ";
            csv_file << nodeId << " , ";
            csv_file << FLAGS_pageProviderThreads << " , ";
            csv_file << FLAGS_coolingPercentage << " , ";
            csv_file << FLAGS_evictCoolestEpochs << " , ";
            csv_file << FLAGS_freePercentage << " , ";
            csv_file << FLAGS_tag << " , ";
            // -------------------------------------------------------------------------------------
            // csv_file << wlInfo.experiment << " , ";
            // csv_file << wlInfo.elements << " , ";
            wlInfo.csv(csv_file);
            // -------------------------------------------------------------------------------------

            for (uint64_t c_i = 0; c_i < RDMACounters::COUNT - 1; c_i++) {
               csv_file << rdmaCounterAgg[c_i] << " ,  ";
            }
            csv_file << rdmaCounterAgg[RDMACounters::COUNT - 1] << std::endl;
         }
         // -------------------------------------------------------------------------------------
         // reset
         // -------------------------------------------------------------------------------------
         std::fill(workerCounterAgg.begin(), workerCounterAgg.end(), 0);
         cpuCountersAgg.clear();
         wait_until_next(next);
         next += 1s;
      }
   }

   std::atomic<bool> running = true;
};

}  // namespace profiling
}  // namespace na
