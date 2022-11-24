#pragma once
// -------------------------------------------------------------------------------------
#include "Defs.hpp"
#include "farm/utils/FarmHelper.hpp"

// -------------------------------------------------------------------------------------
namespace farm {
namespace rdma {
// -------------------------------------------------------------------------------------
enum class MESSAGE_TYPE : uint8_t {
   Empty = 0,  // 0 initialized
   Finish = 1,
   RemoteWrite = 2,
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   Init = 99,
};
// -------------------------------------------------------------------------------------
enum class RESULT : uint8_t {
   COMMITTED = 0,
   ABORTED = 1,
};
// -------------------------------------------------------------------------------------
// INIT Message is exchanged via RDMA S/R hence not in inheritance hierarchy
// -------------------------------------------------------------------------------------
struct TableInfo{
   uintptr_t offset;
   uint64_t begin;
   uint64_t end;
};
   
struct InitMessage {
   uintptr_t mbOffset;  // rdma offsets on remote
   uintptr_t plOffset;
   uintptr_t mbResponseOffset;  // for page provider only
   uintptr_t plResponseOffset;
   uintptr_t barrierAddr;
   NodeID nodeId;  // node id of buffermanager the initiator belongs to
   uint64_t threadId;
   uint64_t num_tables;
   TableInfo tables[MAX_TABLES];
};
// -------------------------------------------------------------------------------------
// Protocol Messages
// -------------------------------------------------------------------------------------
struct Message {
   MESSAGE_TYPE type;
   Message() : type(MESSAGE_TYPE::Empty) {}
   Message(MESSAGE_TYPE type) : type(type) {}
};
// -------------------------------------------------------------------------------------
struct FinishRequest : public Message {
   FinishRequest() : Message(MESSAGE_TYPE::Finish) {}
};

// -------------------------------------------------------------------------------------
struct RemoteWriteRequest : public Message{
   RemoteWriteRequest() : Message(MESSAGE_TYPE::RemoteWrite){}
   // hack hard coded as we only sent ycsb tuples or smaller
   uint8_t buffer[3*64];
   uint64_t addr;
   NodeID nodeId;
};

struct RemoteWriteResponse : public Message{
   RemoteWriteResponse() : Message(MESSAGE_TYPE::RemoteWrite){}
   uint64_t addr;
   NodeID nodeId;
   RESULT rc;
   uint8_t receiveFlag = 1;
};

// -------------------------------------------------------------------------------------
// Get size of Largest Message
union ALLDERIVED {
   FinishRequest fm;
   RemoteWriteRequest rwr;
   RemoteWriteResponse rwresp;
};

static constexpr uint64_t LARGEST_MESSAGE = sizeof(ALLDERIVED);

struct MessageFabric {
   template <typename T, class... Args>
   static T* createMessage(void* buffer, Args&&... args) {
      return new (buffer) T(args...);
   }
};

}  // namespace rdma
}  // namespace farm
