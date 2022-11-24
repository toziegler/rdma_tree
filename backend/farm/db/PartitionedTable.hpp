#pragma once
#include "farm/utils/RandomGenerator.hpp"
#include "farm/utils/FarmHelper.hpp"

namespace farm {
namespace db {
using namespace utils; 
struct PartitionedTable {

   // -------------------------------------------------------------------------------------
   PartitionedTable(uint64_t table_id, uint64_t farmRecordSize, uint64_t begin, uint64_t end, NodeID nodeId, rdma::CM<rdma::InitMessage>& cm)
       : table_id(table_id), t_begin(begin), t_end(end), number_rows(end - begin), size_bytes(farmRecordSize * number_rows), nodeId(nodeId) {
      std::cout << (farmRecordSize) << "\n";
      std::cout << size_bytes << "\n";
      local_rows = (std::byte*)cm.getGlobalBuffer().allocate(size_bytes,64);
      ensure((((uintptr_t)(local_rows)) % 64) == 0);
   };
   // -------------------------------------------------------------------------------------
   template <class Record, class FN>
   bool readModifyWrite(uint64_t t_id, FN&& func) {
      [[maybe_unused]] auto& p = threads::Worker::my().tables[table_id].getPartition(t_id);
      uintptr_t addr = getAddr<Record>(t_id);
      while (true) {
         // latch free read a record first
         Record record = threads::Worker::my().latchfreeReadRecord<Record>(p.nodeId, addr);
         func(record);  // modification
         auto rc = threads::Worker::my().writeRecord<Record>(record, p.nodeId, addr);
         if (rc) break;
      }
      return true;
   }
   // -------------------------------------------------------------------------------------
   // unlatched and only local!
   template <class Record, class FN>
   bool bulk_load_local(FN&& data_generator, uint64_t begin = 0, uint64_t end = 0) {
      end = (end == 0) ? number_rows : end; 
      for (uint64_t r_i = begin; r_i < number_rows; r_i++) {
         Record row;
         uint64_t t_id = r_i + t_begin;
         data_generator(t_id, row);
         auto& f_cl = (reinterpret_cast<TypedFaRMTuple<Record>*>(local_rows)[r_i]);
         toFaRM(row,&f_cl); 
      }
      return true;
   }

   // -------------------------------------------------------------------------------------
   // unlatched but multithreaded and only local!
   template <class Record, class FN>
   bool mt_bulk_load_local(uint64_t threadId, FN&& data_generator) {
      const uint64_t blockSize = number_rows / FLAGS_worker;
      auto begin_tid = threadId * blockSize;
      auto end_tid = begin_tid + blockSize;
      if (threadId == FLAGS_worker - 1) end_tid = number_rows;
      bulk_load_local<Record>(data_generator,begin_tid, end_tid);
      return true;
   }
   // -------------------------------------------------------------------------------------
   template <class Record>
   uintptr_t getAddr(uint64_t t_id) {
      auto& p = threads::Worker::my().tables[table_id].getPartition(t_id);
      auto t_idx = t_id - p.begin;
      return p.offset + (t_idx * (sizeof(TypedFaRMTuple<Record>)));
   }

   template <class Record>
   bool lookup(uint64_t t_id, Record& record) {
      auto& p = threads::Worker::my().tables[table_id].getPartition(t_id);
      uintptr_t addr = getAddr<Record>(t_id);
      record = threads::Worker::my().latchfreeReadRecord<Record>(p.nodeId, addr );
      if(record.key != t_id)
         GDB();
      
      return true;
   }
   template <class Record>
   auto* begin(){
      return reinterpret_cast<TypedFaRMTuple<Record>*>(local_rows);
   }

   template <class Record>
   auto* end(){
      std::byte* end_p = local_rows + size_bytes;
      return reinterpret_cast<TypedFaRMTuple<Record>*>(end_p);
   }
   
   template <class Record>
   void printTable() {
      for(auto* it = begin<Record>(); it < end<Record>(); it++){
          Record record;
         fromFaRM(it,record);
         std::cout << " record " << record.toString() << std::endl;
      }
   }

   const uint64_t table_id;
   const uint64_t t_begin;
   const uint64_t t_end;
   const uint64_t number_rows;
   uint64_t size_bytes;
   const NodeID nodeId;

   std::byte* local_rows;
};

}  // namespace db
}  // namespace farm
