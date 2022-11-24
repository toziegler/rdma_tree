# ---------------------------------------------------------------------------
# FaRM
# ---------------------------------------------------------------------------
project(farm)

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------
file(GLOB_RECURSE FARM_CC **.cpp **/**.cpp **.hpp **/**.hpp)
if (NOT UNIX)
    message(SEND_ERROR "unsupported platform")
endif ()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------
add_library(farm STATIC ${FARM_CC})

OPTION(SANI "Compile farm with sanitizers" OFF)
IF(SANI)
  if (CMAKE_BUILD_TYPE MATCHES Debug)
    message("Compiling with Sanitizers")
    target_compile_options(farm PUBLIC -fsanitize=address)
    target_link_libraries(farm asan)
  endif ()
ENDIF(SANI)

target_link_libraries(farm gflags Threads::Threads atomic numa rdmacm ibverbs aio) 
target_include_directories(farm PUBLIC ${SHARED_INCLUDE_DIRECTORY})
target_include_directories(farm PRIVATE ${CMAKE_CURRENT_LIST_DIR})
# ---------------------------------------------------------------------------
set(FARM_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR})
set_property(TARGET farm APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${FARM_INCLUDE_DIR})
