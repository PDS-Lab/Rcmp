cmake_minimum_required(VERSION 3.14)
project(rchms)

if( CMAKE_BUILD_TYPE STREQUAL "Release" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
    message(STATUS "Release Mode")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")
    message(STATUS "Debug Mode")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories("include" "src/include")

# ##################################################
#                         库
# ##################################################

add_library(rchms SHARED
    src/rchms.cc
)
target_link_libraries(
    rchms
    base
)

# ##################################################
#                     可执行文件
# ##################################################

add_subdirectory(src)

add_executable(
    master
    src/master.cc
)
target_link_libraries(
    master
    base
)

add_executable(
    daemon
    src/daemon.cc
)
target_link_libraries(
    daemon
    base
)