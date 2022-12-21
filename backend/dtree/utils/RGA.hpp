#pragma once 
#include <algorithm>
#include <iostream>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>
// Limitations we cannot pass one RGAPtr to another thread as they depend on
// thread local variables
namespace dtree{
namespace utils{
namespace RGA {
template <typename T> struct GuardedPtr {
  uint64_t cid;
  const uint64_t &ref_seen_cids;
  const uint64_t &ref_current_cids;
  T *object_ptr;
  // Remove compiler generated copy semantics.
  GuardedPtr(uint64_t &seen_cids, uint64_t &current_cids)
      : ref_seen_cids(seen_cids), ref_current_cids(current_cids),
        object_ptr(nullptr) {}
  GuardedPtr(GuardedPtr &&other)
      : ref_seen_cids(other.ref_seen_cids),
        ref_current_cids(other.ref_current_cids) {
    std::swap(object_ptr, other.object_ptr);
    cid = other.cid;
  }
  GuardedPtr &operator=(GuardedPtr &&other) {
    std::swap(object_ptr, other.object_ptr);
    cid = other.cid;
    ref_seen_cids = other.ref_seen_cids;
    return *this;
  }
  GuardedPtr(GuardedPtr const &) = delete;
  GuardedPtr &operator=(GuardedPtr const &) = delete;

  // Const correct access owned object
  T *operator->() const {
    if (ref_seen_cids <= cid)
      throw std::runtime_error("Invalid access to rdma memory");
    return object_ptr;
  }
  T &operator*() const {
    if (ref_seen_cids <= cid)
      throw std::runtime_error("Invalid access to rdma memory");
    return *object_ptr;
  }
  void tag_cid() { cid = ref_current_cids; }
  // Access to smart pointer state
  T *get() const { return object_ptr; }
  explicit operator bool() const { return object_ptr; }
};
extern thread_local uint64_t cid_seen;
extern thread_local uint64_t cid_current;
uint64_t get_current_cid();
void inc_current_cid();
uint64_t get_seen_cid();
void inc_seen_cid();
template <class T, class... Args>
static GuardedPtr<T> make_rgaptr(void *memory_ptr, Args &&...params) {
  GuardedPtr<T> ptr(cid_seen, cid_current);
  // placement new
  new (memory_ptr) T(std::forward<Args>(params)...);
  ptr.object_ptr = new (memory_ptr) T(std::forward<Args>(params)...);
  ptr.cid = 0;
  return ptr;
}
} // namespace RGA
} // namespace utils
} // namespace dtree
