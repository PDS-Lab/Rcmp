cmake_minimum_required(VERSION 3.14)
project(rcmp)

set(CMAKE_CXX_STANDARD 17)

if( CMAKE_BUILD_TYPE STREQUAL "Release" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
    message(STATUS "Release Mode")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")
    message(STATUS "Debug Mode")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(Boost REQUIRED COMPONENTS context fiber)

if(Boost_FOUND)
else()
    message(err: Boost fiber not found)
endif()

link_directories("third_party/eRPC/")
include_directories("include" "src/include" "third_party")
add_subdirectory(src)

# ##################################################
#                         库
# ##################################################

add_library(rcmp SHARED
    src/rcmp.cc
)
target_link_libraries(
    rcmp
    base
)

# ##################################################
#                     可执行文件
# ##################################################

add_executable(
    rcmp_master
    src/master.cc
)
target_link_libraries(
    rcmp_master
    base
)

add_executable(
    rcmp_daemon
    src/daemon.cc
)
target_link_libraries(
    rcmp_daemon
    base
)

add_executable(
    rchfs
    fs/rchfs.cc
)
target_link_libraries(
    rchfs
    rcmp
    fuse3
)

add_subdirectory(test)
