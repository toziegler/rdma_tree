#include "ScrambledZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace farm
{
namespace utils
{

// -------------------------------------------------------------------------------------
u64 ScrambledZipfGenerator::rand()
{
   u64 zipf_value = zipf_generator(gen);
   return min + (farm::utils::FNV::hash(zipf_value) % n);
}
// -------------------------------------------------------------------------------------
u64 ScrambledZipfGenerator::rand(u64 offset)
{
   u64 zipf_value = zipf_generator(gen);
   return (min + ((farm::utils::FNV::hash(zipf_value + offset)) % n));
}

}  // namespace utils
}  // namespace farm
