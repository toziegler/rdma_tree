#include "OneSidedTypes.hpp"
#include "dtree/threads/Worker.hpp"

namespace onesided {

//=== Locking  ===//
template <ConceptObject T>
struct OptimisticLatch : public AbstractLatch<T> {
   using super = AbstractLatch<T>;
   using my_thread = dtree::threads::Worker;

   explicit OptimisticLatch(RemotePtr remote_ptr) : AbstractLatch<T>(remote_ptr) {}
   // because RDMA read atomi w.r.t. to a cache line as per the x68 intel guide
   // this is
   bool try_latch() {
      my_thread::my().nextCache();  // avoids that async unlatch overwrites our cache
      ensure(super::remote_ptr != NULL_REMOTEPTR);
      PageHeader* ph = my_thread::my().read_latch(super::remote_ptr);
      if (ph->remote_latch == EXCLUSIVE_LOCKED) return false;
      super::version = ph->version;
      // Since CL are read consistent we can use the below shortcut
      if constexpr (sizeof(T) <= 32) {
         super::local_copy = static_cast<T*>(ph);
      } else {
         super::local_copy = my_thread::my().remote_read<T>(super::remote_ptr);
         if (super::version != super::local_copy->version) return false;
      }
      return true;
   };

   bool validate() {
      PageHeader* ph = my_thread::my().read_latch(super::remote_ptr);
      if (ph->remote_latch == EXCLUSIVE_LOCKED || ph->version != super::version) return false;
      return true;
   }
};

template <ConceptObject T>
struct ExclusiveLatch : public AbstractLatch<T> {
   explicit ExclusiveLatch(RemotePtr remote_ptr) : AbstractLatch<T>(remote_ptr) {}
   // returns true successfully
   using super = AbstractLatch<T>;
   using my_thread = dtree::threads::Worker;

   explicit ExclusiveLatch(OptimisticLatch<T>&& optmisticLatch) {
      // upgrade logic
   }

   explicit ExclusiveLatch(AbstractLatch<T> alatch) : AbstractLatch<T>(alatch) {
      // special constructor
   }
   bool try_latch() {
      my_thread::my().nextCache();  // avoids that async unlatch overwrites our cache
      ensure(super::remote_ptr != NULL_REMOTEPTR);
      my_thread::my().compareSwapAsync(UNLOCKED, EXCLUSIVE_LOCKED, super::remote_ptr,
                                       dtree::rdma::completion::signaled);
      // read remote data
      super::local_copy = my_thread::my().remote_read<T>(super::remote_ptr);
      super::version = super::local_copy->version;
      bool latched_ = my_thread::my().pollCompletionCS(super::remote_ptr, UNLOCKED);
      if (latched_) super::latched = true;
      return latched_;
   }
   // we have a copy already in optimistic state and want to upgrade the latch 
   // Attention this does leave lock in invalid state and must be called from the GuardX move constructor
   bool try_latch(Version version) {
      my_thread::my().nextCache();  // avoids that async unlatch overwrites our cache
      ensure(super::remote_ptr != NULL_REMOTEPTR);
      auto latched_ = my_thread::my().compareSwap(UNLOCKED, EXCLUSIVE_LOCKED, super::remote_ptr,
                                       dtree::rdma::completion::signaled);
      if(!latched_) return false;
      super::latched = true; // important for unlatch
      PageHeader* ph = my_thread::my().read_latch(super::remote_ptr);
      if (ph->version != version) return false;
      super::version = ph->version;
      return latched_;
   }
   
   void unlatch() {
      // unlatch increments thread local buffer to avoid memory corruption
      ensure(super::latched);
      // increment version
      (super::version)++;
      // write back node
      super::local_copy->version = super::version;
      my_thread::my().remote_write<T>(super::remote_ptr, super::local_copy, dtree::rdma::completion::unsignaled);
      my_thread::my().compareSwap(EXCLUSIVE_LOCKED, UNLOCKED, super::remote_ptr, dtree::rdma::completion::unsignaled);
      super::latched = false;
   };
};

template <ConceptObject T>
struct AllocationLatch : public AbstractLatch<T> {
   AllocationLatch() {
      // read remote data
      my_thread::my().nextCache();  // avoids that async unlatch overwrites our cache
      if (my_thread::my().remote_pages.empty()) { my_thread::my().refresh_caches(); }
      if (!my_thread::my().remote_pages.try_pop(super::remote_ptr))
         throw std::logic_error("could not get a new remote page");
      onesided::allocateInRDMARegion(static_cast<T*>(static_cast<void*>(my_thread::my().get_current_buffer())));
      super::local_copy = static_cast<T*>(static_cast<void*>(my_thread::my().get_current_buffer()));
      super::version = super::local_copy->version;
      super::latched = true;
   }
   // returns true successfully
   using super = AbstractLatch<T>;
   using my_thread = dtree::threads::Worker;

   void unlatch() {
      ensure(super::latched);
      // unlatch increments thread local buffer to avoid memory corruption
      // increment version
      (super::version)++;
      // write back node
      super::local_copy->version = super::version;
      my_thread::my().remote_write<T>(super::remote_ptr, super::local_copy, dtree::rdma::completion::unsignaled);
      my_thread::my().compareSwap(EXCLUSIVE_LOCKED, UNLOCKED, super::remote_ptr, dtree::rdma::completion::unsignaled);
      super::latched = false;
   };
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

   GuardO(GuardO&& other) {
      ensure(!other.moved);
      latch = other.latch;
      moved = other.moved;
   }

   // move assignment operator
   GuardO& operator=(GuardO&& other) {
      if (!moved) checkVersionAndRestart();
      latch = other.latch;
      moved = other.moved;
      other.moved = true;
      other.latch.latched = false;
      other.latch.remote_ptr = NULL_REMOTEPTR;
      other.latch.version = 0;
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
   ~GuardO() noexcept(false) { checkVersionAndRestart(); }

   T* operator->() {
      ensure(!moved);
      return latch.local_copy;
   }

   void release() {  // optimistic unlock
      checkVersionAndRestart();
      moved = true;
      latch.latched = false;
      latch.remote_ptr = NULL_REMOTEPTR;
      latch.version = 0;
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

   explicit GuardX(GuardO<T>&& other) : latch(NULL_REMOTEPTR) {
      ensure(!other.moved);
      latch = ExclusiveLatch<T>(other.latch.remote_ptr);
      // now we officially handed the latch over
      ensure(latch.remote_ptr == other.latch.remote_ptr);
      ensure(latch.latched == false);
      other.moved = true;
      if (!latch.try_latch(other.latch.version)) {
         std::cout << "moved " << latch.local_copy << std::endl;
         std::cout << "o moved " << other.latch.local_copy << std::endl;
         std::cout << "Triggered OLC Restart" << std::endl;
         throw OLCRestartException();
      }
      moved = false;
      latch.local_copy = other.latch.local_copy;
      latch.version = other.latch.version;
      latch.latched = true;
      other.latch.latched = false;
      other.latch.remote_ptr = NULL_REMOTEPTR;
      other.latch.version = 0;
   }

   // assignment operator
   GuardX& operator=(const GuardX&) = delete;

   // move assignment operator
   GuardX& operator=(GuardX&& other) {
      if (!moved) {
         ensure(latch.latched);
         latch.unlatch();
      }
      latch = other.latch;
      moved = false;
      other.moved = true;
      other.latch.latched = false;
      other.latch.remote_ptr = NULL_REMOTEPTR;
      other.latch.version = 0;
      return *this;
   }

   // copy constructor
   GuardX(const GuardX&) = delete;

   // destructor
   ~GuardX() {
      if (!moved && latch.latched){
         std::cout << "unlatch"  << std::endl;
         latch.unlatch();
      }else{
         std::cout << "destructor no unlatch" << std::endl;
      }
   }

   T* operator->() {
      ensure(!moved);
      return latch.local_copy;
   }

   void release() {
      if (!moved) {
         latch.unlatch();
         moved = true;
      }
   }
};

}  // namespace onesided
