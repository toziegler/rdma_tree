#include "OneSidedTypes.hpp"
#include "dtree/threads/Worker.hpp"

namespace onesided {

template <typename T>
concept ConceptObject = std::is_base_of<PageHeader, T>::value;

//=== Locking  ===//

template <ConceptObject T>
struct AbstractLatch {
   RemotePtr remote_ptr;
   Version version{0};
   T* local_copy;
   explicit AbstractLatch(RemotePtr remote_address) : remote_ptr(remote_address) {}
};

template <ConceptObject T>
struct OptimisticLatch : public AbstractLatch<T> {
   using super = AbstractLatch<T>;
   using my_thread = dtree::threads::Worker;

   explicit OptimisticLatch(RemotePtr remote_ptr) : AbstractLatch<T>(remote_ptr) {}
   // because RDMA read atomi w.r.t. to a cache line as per the x68 intel guide
   // this is
   bool try_latch() {
      // read version and lock check if it is latched already
      PageHeader* ph = my_thread::my().read_latch(super::remote_ptr);
      if (ph->remote_latch == EXCLUSIVE_LOCKED) return false;
      super::version = ph->version;
      // read rest of data
      super::local_copy = my_thread::my().remote_read<T>(super::remote_ptr);
      // here we can early abort if version changed already
      if (super::version != super::local_copy->version) return false;
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

   bool try_latch() {
      my_thread::my().compareSwapAsync(UNLOCKED, EXCLUSIVE_LOCKED, super::remote_ptr,
                                       dtree::rdma::completion::signaled);
      // read remote data
      super::local_copy = my_thread::my().remote_read<T>(super::remote_ptr);
      super::version = super::local_copy->version;
      bool latched = my_thread::my().pollCompletionCS(super::remote_ptr, UNLOCKED);
      return latched;
   }

   void unlatch() {
      // unlatch increments thread local buffer to avoid memory corruption
      // increment version
      (super::version)++;
      // write back node
      super::local_copy->version = super::version;
      my_thread::my().remote_write<onesided::MetadataPage>(super::remote_ptr, super::local_copy,
                                                           dtree::rdma::completion::unsignaled);
      my_thread::my().compareSwap(EXCLUSIVE_LOCKED, UNLOCKED, super::remote_ptr, dtree::rdma::completion::unsignaled);
      my_thread::my().nextCache();  // avoids that async unlatch overwrites our cache
   };
};

}  // namespace onesided
