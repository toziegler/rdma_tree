project_headers += files(
  'Config.hpp',
  'Storage.hpp',
  'Compute.hpp',
  
)
project_sources += files(
  'Config.cpp',
  'Storage.cpp',
  'Compute.cpp',
)
project_mains += files(
)

project_includes += include_directories('../')

subdir('db')
subdir('profiling')
subdir('rdma')
subdir('syncprimitives')
subdir('threads')
subdir('utils')

