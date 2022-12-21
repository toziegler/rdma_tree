#include "RGA.hpp"

namespace dtree
{
namespace utils
{
namespace RGA
{
thread_local uint64_t cid_seen{0};
thread_local uint64_t cid_current{0};
uint64_t get_current_cid() { return cid_current; }
void inc_current_cid() { cid_current++; }
uint64_t get_seen_cid() { return cid_seen; }
void inc_seen_cid() { cid_seen++; }
} 
}
}
