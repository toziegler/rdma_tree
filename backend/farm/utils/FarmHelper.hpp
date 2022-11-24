#pragma once
// -------------------------------------------------------------------------------------
#include <cassert>
#include <iostream>
#include <memory_resource>
#include <cmath>
#include <cstring> 
#include "farm/syncprimitives/HybridLatch.hpp"
// -------------------------------------------------------------------------------------
namespace farm
{
namespace utils
{

struct alignas(64) FaRM_cacheline {
   storage::OptimisticLatch latch;
   std::byte buffer[64 - sizeof(storage::OptimisticLatch)];

   bool compareVersion(uint64_t version){
      return latch.checkOrRestart(version);
   }
};
static_assert(sizeof(FaRM_cacheline) == 64);
// -------------------------------------------------------------------------------------

template <size_t N>
struct FaRMTuple{
   FaRM_cacheline cachelines[N];

   FaRMTuple() = default;
   FaRMTuple(FaRMTuple const& rhs){
      for(uint64_t cl_i = 0; cl_i < N; cl_i++){
         std::copy(std::begin(cachelines[cl_i].buffer), std::end(cachelines[cl_i].buffer), std::begin(rhs.cachelines[cl_i].buffer));
      }
   }
   FaRMTuple& operator=(FaRMTuple& rhs){
      for(uint64_t cl_i = 0; cl_i < N; cl_i++){
         std::copy(std::begin(cachelines[cl_i].buffer), std::end(cachelines[cl_i].buffer), std::begin(rhs.cachelines[cl_i].buffer));
      }
      return *this;
   }
          
   bool compareVersions(FaRMTuple& tuple){
      storage::OptimisticLatch& l_latch = cachelines[0].latch;
      storage::OptimisticLatch& r_latch = tuple.cachelines[0].latch;
      return (l_latch.typeVersionLockObsolete.load() == r_latch.typeVersionLockObsolete.load());
   }

   bool latch(){
      uint64_t latched{0};
      for (auto& cl : cachelines) {
         auto rc = cl.latch.tryLatchExclusive();
         if (!rc) {
            if (latched > 0) throw std::logic_error("should not happen");  // if we latched the head then the others should follow
            return false;
         }
         latched++;
      }
      return true;
   }

   void unlatch(){
      // in reverse order
      for (int64_t cl_i = N-1; cl_i >=0; cl_i--) {
         cachelines[cl_i].latch.unlatchExclusive();
      }
   }
   
   bool consistent(){
      // validate the versions
      storage::OptimisticLatch& head = cachelines[0].latch;
      storage::Version v_head = head.typeVersionLockObsolete.load();
      if(head.isLatched(v_head)){
         return false;
      }
      for(auto& cl : cachelines){
         if(!cl.latch.checkOrRestart(v_head))
            return false;
      }
      // check once more to ensure that is all versions has been consistent
      if(head.isLatched(v_head)){
         return false;
      }
      return true;
   }
};

// -------------------------------------------------------------------------------------
template <typename T>
inline constexpr uint64_t numberFaRMCachelines() {
   return std::ceil(sizeof(T) / (double)sizeof(FaRM_cacheline::buffer));
}
// -------------------------------------------------------------------------------------
template<typename T> 
using TypedFaRMTuple = FaRMTuple<numberFaRMCachelines<T>()>;

// -------------------------------------------------------------------------------------
// dst must be ptr to cachelines, cannot use reference due to multiple
template <typename T>
void toFaRM(T& src, FaRMTuple<numberFaRMCachelines<T>()>* dst) {
   auto s = sizeof(T);
   auto* src_ptr = reinterpret_cast<std::byte*>(&src);
   for (auto& cl : dst->cachelines) {
      auto c_s = (s > sizeof(FaRM_cacheline::buffer)) ? sizeof(FaRM_cacheline::buffer) : s;
      std::memcpy(cl.buffer, src_ptr, c_s);
      // advance
      src_ptr = src_ptr + c_s;
      s -= c_s;
      dst++;  // get next FaRM cacheline
      // XXX include check that we do not go out of bounds
   }
   assert(s == 0);
}

template <typename T>
void fromFaRM(FaRMTuple<numberFaRMCachelines<T>()>* src, T& dst) {
   auto s = sizeof(T);
   auto* dst_ptr = reinterpret_cast<std::byte*>(&dst);
   for (auto& cl : src->cachelines) {
      auto c_s = (s > sizeof(FaRM_cacheline::buffer)) ? sizeof(FaRM_cacheline::buffer) : s;
      std::memcpy(dst_ptr, cl.buffer, c_s);
      // advance
      dst_ptr = dst_ptr + c_s;
      s -= c_s;
      src++;  // XXX include check that we do not go out of bounds
   }
}

// -------------------------------------------------------------------------------------
// dst must be ptr to cachelines, cannot use reference due to multiple
template <typename T>
void toFaRM(T& src, FaRM_cacheline* dst) {
   auto cl_n = numberFaRMCachelines<T>();
   auto s = sizeof(T);
   auto* src_ptr = reinterpret_cast<std::byte*>(&src);
   for (uint64_t cl_i = 0; cl_i < cl_n; cl_i++) {
      auto c_s = (s > sizeof(FaRM_cacheline::buffer)) ? sizeof(FaRM_cacheline::buffer) : s;
      std::memcpy(dst->buffer, src_ptr, c_s);
      // advance
      src_ptr = src_ptr + c_s;
      s -= c_s;
      dst++;  // get next FaRM cacheline
      // XXX include check that we do not go out of bounds
   }
   assert(s == 0);
}

template <typename T>
void fromFaRM(FaRM_cacheline* src, T& dst) {
   auto cl_n = numberFaRMCachelines<T>();
   auto s = sizeof(T);
   auto* dst_ptr = reinterpret_cast<std::byte*>(&dst);
   for (uint64_t cl_i = 0; cl_i < cl_n; cl_i++) {
      auto c_s = (s > sizeof(FaRM_cacheline::buffer)) ? sizeof(FaRM_cacheline::buffer) : s;
      std::memcpy(dst_ptr, src->buffer, c_s);
      // advance
      dst_ptr = dst_ptr + c_s;
      s -= c_s;
      src++;  // XXX include check that we do not go out of bounds
   }
}
}
}
