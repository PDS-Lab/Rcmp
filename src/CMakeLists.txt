file(GLOB_RECURSE LIB_SRCS *.cc)

list(FILTER LIB_SRCS EXCLUDE REGEX "/rcmp.cc$")
list(FILTER LIB_SRCS EXCLUDE REGEX "/daemon.cc$")
list(FILTER LIB_SRCS EXCLUDE REGEX "/master.cc$")
list(FILTER LIB_SRCS EXCLUDE REGEX "/test/")

add_library(base STATIC ${LIB_SRCS})
target_link_libraries(base pthread erpc ibverbs numa rdmacm boost_coroutine Boost::fiber Boost::context)

add_subdirectory(test)