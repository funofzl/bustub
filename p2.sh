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

cd build && make && make format && make check-lint && make hash_table_test && ./test/hash_table_test || exit
cd .. || exit

zip project2-submission.zip src/include/buffer/lru_replacer.h \
    src/buffer/lru_replacer.cpp \
    src/include/buffer/buffer_pool_manager_instance.h \
    src/buffer/buffer_pool_manager_instance.cpp \
    src/include/buffer/parallel_buffer_pool_manager.h \
    src/buffer/parallel_buffer_pool_manager.cpp \
    src/include/storage/page/hash_table_directory_page.h \
    src/storage/page/hash_table_directory_page.cpp \
    src/include/storage/page/hash_table_bucket_page.h \
    src/storage/page/hash_table_bucket_page.cpp \
    src/include/container/hash/extendible_hash_table.h \
    src/container/hash/extendible_hash_table.cpp

cd build && make check-clang-tidy
