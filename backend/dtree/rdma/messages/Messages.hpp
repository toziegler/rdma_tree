#pragma once
// -------------------------------------------------------------------------------------
#include "Defs.hpp"
// -------------------------------------------------------------------------------------
namespace dtree {
namespace rdma {
// -------------------------------------------------------------------------------------
enum class MESSAGE_TYPE : uint8_t {
   Empty = 0,  // 0 initialized
   Finish = 1,
   Insert = 2,
   Lookup = 3,
   Scan = 4,
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
   uintptr_t remote_cache_counter;
   uintptr_t remote_cache_offset;
   uintptr_t scanResultOffset; // offset to receive scan result 
   uintptr_t metadataOffset; // only node 0 sends this
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
struct InsertRequest : public Message{
   InsertRequest() : Message(MESSAGE_TYPE::Insert){}
   // hack hard coded as we only sent ycsb tuples or smaller
   Key key;
   Value value;
   NodeID nodeId;
};

struct InsertResponse : public Message{
   InsertResponse() : Message(MESSAGE_TYPE::Insert){}
   RESULT rc;
   uint8_t receiveFlag = 1;
};
// -------------------------------------------------------------------------------------
struct LookupRequest : public Message{
   LookupRequest() : Message(MESSAGE_TYPE::Lookup){}
   // hack hard coded as we only sent ycsb tuples or smaller
   Key key;
   NodeID nodeId;
};

struct LookupResponse : public Message{
   LookupResponse() : Message(MESSAGE_TYPE::Lookup){}
   Value value;
   RESULT rc;
   uint8_t receiveFlag = 1;
};
// -------------------------------------------------------------------------------------
struct ScanRequest : public Message{
   ScanRequest() : Message(MESSAGE_TYPE::Scan){}
   // hack hard coded as we only sent ycsb tuples or smaller
   Key from;
   Key to;
   NodeID nodeId;
};

// result is transferred one sided 
struct ScanResponse : public Message{
   ScanResponse() : Message(MESSAGE_TYPE::Scan){}
   size_t length;
   RESULT rc;
   uint8_t receiveFlag = 1;
};

// -------------------------------------------------------------------------------------
// Get size of Largest Message
union ALLDERIVED {
   FinishRequest fm;
   InsertRequest ir;
   InsertResponse irr;
   LookupRequest lr;
   LookupResponse lrr;
   ScanRequest scr;
   ScanResponse scrr;
};

static constexpr uint64_t LARGEST_MESSAGE = sizeof(ALLDERIVED);

struct MessageFabric {
   template <typename T, class... Args>
   static T* createMessage(void* buffer, Args&&... args) {
      return new (buffer) T(args...);
   }
};

}  // namespace rdma
}  // namespace dtree
