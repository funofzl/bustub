#!/bin/bash
###
# @Author: lxk
# @Date: 2022-08-03 18:16:29
# @LastEditors: lxk
# @LastEditTime: 2022-08-03 21:41:47
###
pwd=$(dirname $0)
cd $pwd || exit
zip project1-submission.zip \
    src/include/buffer/lru_replacer.h \
    src/buffer/lru_replacer.cpp \
    src/include/buffer/buffer_pool_manager_instance.h \
    src/buffer/buffer_pool_manager_instance.cpp \
    src/include/buffer/parallel_buffer_pool_manager.h \
    src/buffer/parallel_buffer_pool_manager.cpp
