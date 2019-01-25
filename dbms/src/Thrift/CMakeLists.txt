
include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_thrift .)

add_library(clickhouse_thrift STATIC  ${clickhouse_thrift_headers} ${clickhouse_thrift_sources})

target_link_libraries (clickhouse_thrift ${ClickHouse_SOURCE_DIR}/contrib/thrift_install/lib/libthrift.a)



target_include_directories (clickhouse_thrift  PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/thrift_install/include)