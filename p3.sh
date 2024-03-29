#!/bin/bash
###
# @Author: lxk
# @Date: 2022-08-03 18:16:29
# @LastEditors: lxk
# @LastEditTime: 2022-08-06 16:07:53
###
pwd=$(dirname $0)
echo $pwd
cd $pwd || exit

cd build && make && make format && make check-lint && make executor_test && ./test/executor_test || exit
cd .. || exit

zip project3-submission.zip \
src/include/buffer/lru_replacer.h \
src/buffer/lru_replacer.cpp \
src/include/buffer/buffer_pool_manager_instance.h \
src/buffer/buffer_pool_manager_instance.cpp \
src/include/storage/page/hash_table_directory_page.h \
src/storage/page/hash_table_directory_page.cpp \
src/include/storage/page/hash_table_bucket_page.h \
src/storage/page/hash_table_bucket_page.cpp \
src/include/container/hash/extendible_hash_table.h \
src/container/hash/extendible_hash_table.cpp \
src/include/execution/execution_engine.h \
src/include/execution/executors/seq_scan_executor.h \
src/include/execution/executors/insert_executor.h \
src/include/execution/executors/update_executor.h \
src/include/execution/executors/delete_executor.h \
src/include/execution/executors/nested_loop_join_executor.h \
src/include/execution/executors/hash_join_executor.h \
src/include/execution/executors/aggregation_executor.h \
src/include/execution/executors/limit_executor.h \
src/include/execution/executors/distinct_executor.h \
src/execution/seq_scan_executor.cpp \
src/execution/insert_executor.cpp \
src/execution/update_executor.cpp \
src/execution/delete_executor.cpp \
src/execution/nested_loop_join_executor.cpp \
src/execution/hash_join_executor.cpp \
src/execution/aggregation_executor.cpp \
src/execution/limit_executor.cpp \
src/execution/distinct_executor.cpp \
src/include/storage/page/tmp_tuple_page.h

cd build && make check-clang-tidy
