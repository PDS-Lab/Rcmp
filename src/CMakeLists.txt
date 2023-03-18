aux_source_directory(. LIB_SRCS)

list(FILTER LIB_SRCS EXCLUDE REGEX "rchms.cc")
list(FILTER LIB_SRCS EXCLUDE REGEX "daemon.cc")
list(FILTER LIB_SRCS EXCLUDE REGEX "master.cc")

add_library(base STATIC ${LIB_SRCS})

add_subdirectory(test)