add_executable(frontend frontend.cpp)
add_dependencies(frontend farm)
target_link_libraries(frontend farm numa)

add_executable(ycsb ycsb/ycsb.cpp)
add_dependencies(ycsb farm)
target_link_libraries(ycsb farm numa)


add_executable(ycsb_latency ycsb/ycsb_latency.cpp)
add_dependencies(ycsb_latency farm)
target_link_libraries(ycsb_latency farm numa)


