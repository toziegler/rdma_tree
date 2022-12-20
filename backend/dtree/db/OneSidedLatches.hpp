#include <stdexcept>

#include "OneSidedTypes.hpp"
#include "dtree/threads/Worker.hpp"
namespace dtree {
namespace onesided {

template <typename T>
concept ConceptObject = std::is_base_of<PageHeader, T>::value;
template <ConceptObject T>
struct AbstractLatch {
   RemotePtr remote_ptr{0, 0};
   Version version{0};
   bool latched{false};
   bool moved{false};
   RDMAMemoryInfo rdma_mem;  // local rdma memory
   explicit AbstractLatch(RemotePtr remote_address) : remote_ptr(remote_address) {
      // std::cout << "Constructor AbstractLatch" << std::endl;
      auto success = threads::onesided::Worker::my().local_rmemory.try_pop(rdma_mem);
      if (!success) throw std::runtime_error("Maximum latch depth reached");
   }
   AbstractLatch& operator=(AbstractLatch&& other) {
      // std::cout << "move assingment" << std::endl;
      ensure(!moved);
      ensure(!other.moved);
      other.moved = true;
      remote_ptr = other.remote_ptr;
      version = other.version;
      latched = other.latched;
      rdma_mem = other.rdma_mem;
      other.version = 0;
      other.latched = false;
      return *this;
   }
   explicit AbstractLatch(AbstractLatch&& other) {
      // std::cout << "Moved constructor" << std::endl;
      ensure(!moved);
      ensure(!other.moved);
      other.moved = true;
      remote_ptr = other.remote_ptr;
      version = other.version;
      latched = other.latched;
      rdma_mem = other.rdma_mem;
      other.version = 0;
      other.latched = false;
   }

   bool isLatched() { return latched; }

   ~AbstractLatch() {
      if (!moved) {
         //  return memory
         [[maybe_unused]] auto s = threads::onesided::Worker::my().local_rmemory.try_push(rdma_mem);
      } else {
         // std::cout << "Moved Destructor " << std::endl;
      }
   }
};

//=== Locking  ===//
template <ConceptObject T>
struct OptimisticLatch : public AbstractLatch<T> {
   using super = AbstractLatch<T>;
   using my_thread = dtree::threads::onesided::Worker;
   explicit OptimisticLatch(RemotePtr remote_ptr) : AbstractLatch<T>(remote_ptr) {
      // std::cout << "OptimisticLatch  Constructor" << std::endl;
   }
   explicit OptimisticLatch(OptimisticLatch&& o_other) : AbstractLatch<T>(std::move(o_other)) {
      // std::cout << "OptimisticLatch Move Constructor" << std::endl;
   }
   ~OptimisticLatch() {
      if (!super::moved) std::cout << "OptimisticLatch destructor" << std::endl;
   }
   OptimisticLatch& operator=(OptimisticLatch&& other) {
      *static_cast<AbstractLatch<T>*>(this) = std::move(*static_cast<AbstractLatch<T>*>(&other));
      return *this;
   }

   OptimisticLatch& operator=(OptimisticLatch& other) = delete;
   OptimisticLatch(OptimisticLatch& other) = delete;  // copy constructor
   // because RDMA read atomi w.r.t. to a cache line as per the x68 intel guide
   bool try_latch() {
      ensure(super::remote_ptr != NULL_REMOTEPTR);
      my_thread::my().read_latch(super::remote_ptr, super::rdma_mem.latch_buffer);
      auto* ph = static_cast<PageHeader*>(super::rdma_mem.latch_buffer);
      if (ph->remote_latch == EXCLUSIVE_LOCKED) return false;
      super::version = ph->version;
      // Since CL are read consistent we can use the below shortcut
      if constexpr (sizeof(T) <= 32) {
         super::rdma_mem.local_copy = static_cast<T*>(ph);
      } else {
         my_thread::my().remote_read<T>(super::remote_ptr, static_cast<T*>(super::rdma_mem.local_copy));
         if (super::version != super::rdma_mem.local_copy->version) return false;
      }
      return true;
   };

   bool validate() {
      my_thread::my().read_latch(super::remote_ptr, super::rdma_mem.latch_buffer);
      auto* ph = static_cast<PageHeader*>(static_cast<void*>(super::rdma_mem.latch_buffer));
      if (ph->remote_latch == EXCLUSIVE_LOCKED || ph->version != super::version) return false;
      return true;
   }
};
template <ConceptObject T>
struct ExclusiveLatch : public AbstractLatch<T> {
   // returns true successfully
   using super = AbstractLatch<T>;
   using my_thread = dtree::threads::onesided::Worker;
   explicit ExclusiveLatch(RemotePtr remote_ptr) : AbstractLatch<T>(remote_ptr) {}
   explicit ExclusiveLatch(OptimisticLatch<T>&& o_other) : AbstractLatch<T>(std::move(o_other)) {
      // std::cout << "moved (update) constructor latch" << std::endl;
   }
   // delete constuctors we do not want
   ExclusiveLatch& operator=(ExclusiveLatch& other) = delete;
   ExclusiveLatch(ExclusiveLatch& other) = delete;   // copy constructor
   ExclusiveLatch(ExclusiveLatch&& other) = delete;  // move constructor
   bool version_mismatch{false};

   ExclusiveLatch& operator=(ExclusiveLatch&& other) {
      *static_cast<AbstractLatch<T>*>(this) = std::move(*static_cast<AbstractLatch<T>*>(&other));
      return *this;
   }
   bool try_latch() {
      ensure(super::remote_ptr != NULL_REMOTEPTR);
      my_thread::my().compareSwapAsync(UNLOCKED, EXCLUSIVE_LOCKED, super::remote_ptr, dtree::rdma::completion::signaled,
                                       &super::rdma_mem.latch_buffer->remote_latch);
      // read remote data
      my_thread::my().remote_read<T>(super::remote_ptr, static_cast<T*>(super::rdma_mem.local_copy));
      super::version = super::rdma_mem.local_copy->version;
      bool latched_ = my_thread::my().pollCompletionAsyncCAS(super::remote_ptr, UNLOCKED,
                                                             &super::rdma_mem.latch_buffer->remote_latch);
      if (latched_) super::latched = true;
      return latched_;
   }
   // we have a copy already in optimistic state and want to upgrade the latch
   // Attention this does leave lock in invalid state and must be called from the GuardX move constructor
   bool try_latch(Version version) {
      ensure(super::remote_ptr != NULL_REMOTEPTR);
      ensure(!super::latched);
      auto latched_ =
          my_thread::my().compareSwap(UNLOCKED, EXCLUSIVE_LOCKED, super::remote_ptr, dtree::rdma::completion::signaled,
                                      &super::rdma_mem.latch_buffer->remote_latch);
      if (!latched_) { return false; }
      super::latched = true;  // important for unlatch
      // todo check if this could be ab bug since we write back our old state in the unlatch although we have the wrong
      // version
      my_thread::my().read_latch(super::remote_ptr, super::rdma_mem.latch_buffer);
      auto* ph = static_cast<PageHeader*>(static_cast<void*>(super::rdma_mem.latch_buffer));
      ensure(ph->remote_latch == EXCLUSIVE_LOCKED);
      if (ph->version != version) {
         version_mismatch = true;
         return false;
      }
      // upgrade header of read data
      super::version = ph->version;
      // now we upgrade the old header with the new one
      *super::rdma_mem.local_copy = *ph;
      return latched_;
   }

   void unlatch() {
      // unlatch increments thread local buffer to avoid memory corruption
      ensure(super::latched);
      // increment version
      if (!version_mismatch) {
         (super::version)++;
         super::rdma_mem.local_copy->version = super::version;
         ensure(static_cast<T*>(super::rdma_mem.local_copy)->remote_latch == EXCLUSIVE_LOCKED);
         my_thread::my().remote_write<T>(super::remote_ptr, static_cast<T*>(super::rdma_mem.local_copy),
                                         dtree::rdma::completion::signaled);
      }  // unsignaled could improve the performance but the memory management gets annoying as memory used for async.
         // op
      // should not be returned in destructor before the completion finished
      ensure(my_thread::my().compareSwap(EXCLUSIVE_LOCKED, UNLOCKED, super::remote_ptr,
                                         dtree::rdma::completion::signaled,
                                         &super::rdma_mem.latch_buffer->remote_latch));
      super::latched = false;
   };
};

template <ConceptObject T>
struct AllocationLatch : public AbstractLatch<T> {
   // returns true successfully
   using super = AbstractLatch<T>;
   using my_thread = dtree::threads::onesided::Worker;
   AllocationLatch() : AbstractLatch<T>(NULL_REMOTEPTR) {
      // read remote data
      if (my_thread::my().remote_pages.empty()) { my_thread::my().refresh_caches(); }
      if (!my_thread::my().remote_pages.try_pop(super::remote_ptr))
         throw std::logic_error("could not get a new remote page");
      onesided::allocateInRDMARegion(static_cast<T*>(static_cast<void*>(super::rdma_mem.local_copy)));
      super::latched = true;
   }

   void unlatch() {
      ensure(super::latched);
      // unlatch increments thread local buffer to avoid memory corruption
      // increment version
      (super::version)++;
      // write back node
      super::rdma_mem.local_copy->version = super::version;
      // todo unsignaled
      my_thread::my().remote_write<T>(super::remote_ptr, static_cast<T*>(super::rdma_mem.local_copy),
                                      dtree::rdma::completion::signaled);
      my_thread::my().compareSwap(EXCLUSIVE_LOCKED, UNLOCKED, super::remote_ptr, dtree::rdma::completion::signaled,
                                  &super::rdma_mem.latch_buffer->remote_latch);
      super::latched = false;
   };
   T* operator->() {
      ensure(super::latched);
      return static_cast<T*>(super::rdma_mem.local_copy);
   }
};

template <typename T>
struct GuardO {
   OptimisticLatch<T> latch;
   bool moved = false;

   GuardO() : latch(NULL_REMOTEPTR), moved(true) {}

   explicit GuardO(RemotePtr rptr) : latch(rptr), moved(false) {
      // try to get optimistic latch until it is no longer latched
      while (!latch.try_latch())
         ;
   }

   // TODO check again
   template <class T2>
   GuardO(RemotePtr current, GuardO<T2>& parent) {
      ensure(!parent.moved);
      parent.checkVersionAndRestart();
      this->latch = OptimisticLatch<T>(current);
      while (!this->latch.try_latch())
         ;
   }

   GuardO(GuardO&& other) : latch(std::move(other.latch)) {
      ensure(!other.moved);
      other.moved = true;
   }

   // move assignment operator
   GuardO& operator=(GuardO&& other) {
      // std::cout << "GuardO move assignment " << std::endl;
      if (!moved) checkVersionAndRestart();
      // here we need to return the memory of our current latch
      [[maybe_unused]] auto s = threads::onesided::Worker::my().local_rmemory.try_push(latch.rdma_mem);
      latch = std::move(other.latch);  // calls move assignment
      other.moved = true;
      return *this;
   }

   // assignment operator
   GuardO& operator=(const GuardO&) = delete;

   // copy constructor
   GuardO(const GuardO&) = delete;

   void checkVersionAndRestart() {
      if (!moved) {
         if (latch.validate()) return;
         if (std::uncaught_exceptions() == 0) throw OLCRestartException();
      }
   }

   // destructor
   ~GuardO() noexcept(false) {
      if (!moved) { checkVersionAndRestart(); }
   }

   T* operator->() {
      ensure(!moved);
      return static_cast<T*>(latch.rdma_mem.local_copy);
   }
};

template <class T>
struct GuardX {
   ExclusiveLatch<T> latch;
   bool moved{false};
   // constructor
   GuardX() : latch(NULL_REMOTEPTR), moved(true) {}

   // constructor
   explicit GuardX(RemotePtr rptr) : latch(rptr), moved(false) {
      while (!latch.try_latch())
         ;
   }

   explicit GuardX(GuardO<T>&& other) : latch(std::move(other.latch)) {
      ensure(latch.remote_ptr == other.latch.remote_ptr);
      ensure(latch.latched == false);
      ensure(other.latch.moved);
      other.moved = true;
      ensure(!moved);
      ensure(!latch.latched);
      if (!latch.try_latch(latch.version)) {
         if (latch.latched) latch.unlatch();  // need to call unlatch since destructor is not called here
         throw OLCRestartException();
      }
      ensure(!moved);
   }

   // assignment operator
   GuardX& operator=(const GuardX&) = delete;
   // copy constructor
   GuardX(const GuardX&) = delete;

   // move assignment operator
   GuardX& operator=(GuardX&& other) {
      if (!moved) {
         ensure(latch.latched);
         latch.unlatch();
         [[maybe_unused]] auto s = threads::onesided::Worker::my().local_rmemory.try_push(latch.rdma_mem);
      }
      latch = std::move(other.latch);  // calls move assignment
      other.moved = true;
      ensure(!moved);
      return *this;
   }

   // destructor
   ~GuardX() {
      if (!moved && latch.latched) { latch.unlatch(); }
      // if moved not dealloc
   }

   T* operator->() {
      ensure(!moved);
      return static_cast<T*>(latch.rdma_mem.local_copy);
   }

   void release() {
      if (!moved) {
         latch.unlatch();
         moved = true;
         [[maybe_unused]] auto s = threads::onesided::Worker::my().local_rmemory.try_push(latch.rdma_mem);
      }
   }
};

}  // namespace onesided
}  // namespace dtree
