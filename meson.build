project('rdma_tree', ['c', 'cpp'],
    version: '1.0.0',
    default_options: [
        # 'werror=true',
        # 'warning_level=3',
        'c_std=c11',
        'cpp_std=c++20',
        'buildtype=release',
        # 'b_lto=true',
        # 'buildtype=debug',
        'b_sanitize=address',
    ])
    


# Disable warnings for designated initializers with base class defaults
add_project_arguments([
    '-Wconversion',
    '-Wno-pedantic',
], language: ['c', 'cpp'])

project_headers = []
project_sources = []
project_mains = []

project_includes = [
  include_directories('.'),
  include_directories('shared-headers'),
]

thread_dep = dependency('threads', required : true)

# cmake = import('cmake')
# crow_proj = cmake.subproject('crow')
# crow_dep = crow_proj.dependency('Crow')

cpp_compiler = meson.get_compiler('cpp')

numa_dep = cpp_compiler.find_library('numa', has_headers: ['numa.h'], required: true)
gflags_dep = cpp_compiler.find_library('gflags', required: true)

# libumad = cc.find_library('ibumad', required: get_option('rdma'))
rdma_libs = [
    cpp_compiler.find_library('rdmacm', has_headers: ['rdma/rdma_cma.h'], required: true),
    cpp_compiler.find_library('ibverbs', has_headers: ['infiniband/verbs.h'], required: true),
]
rdma_dep = declare_dependency(dependencies: rdma_libs)

# This triggers the builds of sources
subdir('backend/dtree')
subdir('frontend')

project_deps = [
    thread_dep,
    numa_dep,
    rdma_dep,
    gflags_dep,
]

project_mains += files(
    'btree.cpp',
)

project_sources += files(
  # cpp files no main 
)

lib = static_library('rdma_tree',
    project_sources,
    include_directories: project_includes,
    dependencies: project_deps)

fs = import('fs')


foreach prog_src : project_mains
    prog_name = fs.stem(prog_src)
    link_args = []

    main = executable(f'@prog_name@',
        prog_src, 
        include_directories: project_includes,
        link_args: link_args,
        link_with: [lib],
        dependencies: project_deps
    )
endforeach
