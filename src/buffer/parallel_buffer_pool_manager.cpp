//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : m_pool_size_(pool_size), m_bmp_start_idx_(0) {
  // Allocate and create individual BufferPoolManagerInstances
  //   size_t single_pool_size = pool_size / num_instances;
  //   size_t last_single_pool_size = pool_size - (single_pool_size * num_instances - 1);
  m_managers_.resize(num_instances);
  for (uint32_t i = 0; i < num_instances - 1; i++) {
    m_managers_[i] =
        new BufferPoolManagerInstance(pool_size, static_cast<uint32_t>(num_instances), i, disk_manager, log_manager);
  }
  m_managers_[num_instances - 1] =
      new BufferPoolManagerInstance(pool_size, static_cast<uint32_t>(num_instances),
                                    static_cast<uint32_t>(num_instances - 1), disk_manager, log_manager);
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (auto manager : m_managers_) {
    delete manager;
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return m_managers_.size() * m_pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return m_managers_[page_id % m_managers_.size()];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  // 1. fist get corresponding bufferpool
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  // 因为需要轮训 所以需要加锁
  std::lock_guard<std::mutex> lock(m_latch_);
  Page *page;
  for (size_t i = 0; i < m_managers_.size(); i++) {
    page = m_managers_[m_bmp_start_idx_]->NewPage(page_id);
    m_bmp_start_idx_ = (m_bmp_start_idx_ + 1) % m_managers_.size();  // bump the starting index
    if (page != nullptr) {
      return page;
    }
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *bpm = GetBufferPoolManager(page_id);
  return bpm->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (auto bpm : m_managers_) {
    bpm->FlushAllPages();
  }
}

}  // namespace bustub
