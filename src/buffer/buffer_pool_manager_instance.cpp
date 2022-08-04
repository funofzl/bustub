//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include <cassert>
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::lock_guard<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t flush_fid = iter->second;
  Page *page = pages_ + flush_fid;
  // TODO(后续使用)
  //   if (enable_logging && page->GetLSN() > log_manager_->GetPersistentLSN()) {
  // log_manager_->Flush(true);
  //   }
  disk_manager_->WritePage(page_id, page->data_);
  pages_[flush_fid].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  // 这在最后checkpoint会用到 设置为clean
  for (auto page_frame : page_table_) {
    page_id_t page_id = page_frame.first;
    frame_id_t frame_id = page_frame.second;
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
  }
}

bool BufferPoolManagerInstance::FindReplacer(frame_id_t *frame_id) {
  // 1.    try to find a free frame in free_list
  if (!free_list_.empty()) {
    frame_id_t first_frame_id = free_list_.front();
    free_list_.pop_front();
    *frame_id = first_frame_id;
    return true;
  }
  // 2.    replacer a victim page
  // 2.1   if there are no pages can be replaced, return false
  // 如果没有空闲的frame 就去LRUReplacer找
  if (!replacer_->Victim(frame_id)) {
    return false;  // 没有找到可以替换的frame
  }
  // 2.2   flush log and page
  Page *page = &pages_[*frame_id];
  if (page->IsDirty()) {
    // if (enable_logging && page->GetLSN() > log_manager_->GetPersistentLSN()) {
    // TODO(后续使用)
    //   log_manager_->Flush(true);
    // }
    char *data = pages_[*frame_id].data_;
    disk_manager_->WritePage(page->page_id_, data);
  }
  // 2.3.   Delete R from the page table and Reset metadata in Page.
  page_table_.erase(page->page_id_);
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  return true;  // 找到了可以替换的frame
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);

  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  bool is_all_pinned = true;
  for (int i = 0; i < static_cast<int>(pool_size_); i++) {
    if (pages_[i].pin_count_ == 0) {
      is_all_pinned = false;
      break;
    }
  }
  if (is_all_pinned) {
    return nullptr;
  }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t victim_frame_id;
  if (!FindReplacer(&victim_frame_id)) {
    return nullptr;  // 没有找到frame
  }
  // 0.   Make sure you call AllocatePage!
  page_id_t new_page_id = AllocatePage();

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  Page *victim_page = &pages_[victim_frame_id];
  page_table_[new_page_id] = victim_frame_id;
  victim_page->page_id_ = new_page_id;
  victim_page->pin_count_ = 1;
  victim_page->is_dirty_ = false;
  victim_page->ResetMemory();

  replacer_->Pin(victim_frame_id);

  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = new_page_id;
  return victim_page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->Pin(frame_id);
    return page;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t replace_frame_id;
  if (!FindReplacer(&replace_frame_id)) {
    return nullptr;
  }
  Page *page = &pages_[replace_frame_id];
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  page_table_[page_id] = replace_frame_id;  // 建立我们需要的页的映射关系到替换的frame_id
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  disk_manager_->ReadPage(page_id, page->data_);
  page->is_dirty_ = false;
  page->page_id_ = page_id;
  page->pin_count_++;
  replacer_->Pin(replace_frame_id);

  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  // 1. find this page
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }

  // 2. check if pin_count > 0
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    latch_.unlock();
    return false;
  }
  // 3. delete in disk in here
  DeallocatePage(page_id);

  // 4. reset metadata
  page_table_.erase(page_id);
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  page->ResetMemory();
  replacer_->Pin(frame_id);

  // 5. return it to the free list
  free_list_.push_back(frame_id);
  latch_.unlock();
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  // 1. 如果page_table中就没有
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  // 2. 找到要被unpin的page
  frame_id_t unpinned_fid = iter->second;
  Page *unpinned_page = &pages_[unpinned_fid];

  if (is_dirty) {
    unpinned_page->is_dirty_ = true;
  }
  assert(unpinned_page->pin_count_ > 0);
  unpinned_page->pin_count_--;
  if (unpinned_page->GetPinCount() == 0) {
    replacer_->Unpin(unpinned_fid);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
