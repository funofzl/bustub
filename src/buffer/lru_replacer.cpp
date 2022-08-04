/*
 * @Author: lxk
 * @Date: 2022-08-03 21:13:14
 * @LastEditors: lxk
 * @LastEditTime: 2022-08-04 10:06:21
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : m_capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(m_latch_);
  if (m_list_.empty()) {  // 没有空闲的frame
    m_latch_.unlock();
    return false;
  }
  frame_id_t last_frame_id = m_list_.back();
  *frame_id = last_frame_id;
  m_list_.pop_back();
  m_map_.erase(last_frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(m_latch_);
  if (m_map_.count(frame_id) != 0) {
    m_list_.erase(m_map_[frame_id]);
    m_map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  // 1. 查看是否存在
  std::lock_guard<std::mutex> lock(m_latch_);
  if (m_map_.count(frame_id) != 0) {
    m_latch_.unlock();
    return;
  }
  // if list size >= capacity
  // while {delete front}
  while (Size() >= m_capacity_) {
    frame_id_t need_del = m_list_.front();
    m_list_.pop_front();
    m_map_.erase(need_del);
  }
  // insert
  m_list_.push_front(frame_id);
  m_map_[frame_id] = m_list_.begin();
}

size_t LRUReplacer::Size() { return m_map_.size(); }

}  // namespace bustub
