//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  // 1.   首先分配directory_page 和 初始的bucket_page
  HashTableDirectoryPage *dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_)->GetData());
  // 2.    按照GlobalPage alloc bucket pages.
  page_id_t page_id;
  buffer_pool_manager_->NewPage(&page_id);
  dir_page->SetBucketPageId(0, page_id);
  dir_page->SetLocalDepth(0, 0);

  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}  // namespace bustub

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  assert(directory_page_id_ != INVALID_PAGE_ID);
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  return reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(page != nullptr);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->RLatch();
  bool res = bucket_page->GetValue(key, comparator_, result);
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();

  // Unpin the dir_page and bucket_page
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->WLatch();
  bool fulled = bucket_page->IsFull();
  bool res = false;
  if (!fulled) {
    // if isfulled, no need to try, because it will always return false.
    res = bucket_page->Insert(key, value, comparator_);
  }
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  table_latch_.RUnlock();
  if (fulled) {
    return SplitInsert(transaction, key, value);
  }
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // Bucket page is Full, need to split image and insert.
  // First.   If local_deprth < global_depth, just simplely allocate a new page and rehash the old bucket page.
  // Second.  If local depth = global_depth, need to Incr global_depth and then allocate a new page and rehash the old
  // bucket page.
  //   here Print();
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);

  // 1. Fetch bucket_page and Allocate another new bucket page
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  reinterpret_cast<Page *>(bucket_page)->WLatch();
  if (!bucket_page->IsFull()) {  // 再次检查桶是否满了
    bool ret_tmp = bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    table_latch_.WUnlock();
    return ret_tmp;
  }

  // 2. Judge if need to incr global depth.
  if (dir_page->GetLocalDepth(bucket_idx) == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  // 3. Create a new page
  page_id_t new_bucket_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
  assert(new_page != nullptr);
  new_page->WLatch();
  auto new_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page->GetData());

  // 4.  Incr local_depth and Get split image bucket
  dir_page->IncrLocalDepth(bucket_idx);  // first incr local_depth and then calculate the another bucket_idx
  uint32_t new_bucket_idx = dir_page->GetSplitImageIndex(bucket_idx);
  uint32_t now_local_depth = dir_page->GetLocalDepth(bucket_idx);
  uint32_t now_local_mask = static_cast<uint32_t>((1 << now_local_depth) - 1);
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    page_id_t page_id = dir_page->GetBucketPageId(i);
    if (page_id == bucket_page_id) {
      // two parts all need to incrLocalDepth.
      dir_page->SetLocalDepth(i, now_local_depth);
      // another part need to pointer another page
      if ((i & now_local_mask) != (bucket_idx & now_local_mask)) {
        dir_page->SetBucketPageId(i, new_bucket_page_id);
      }
    }
  }
  //   uint32_t diff = 0x1 << now_local_depth;  // the distance of two buckets which share a page
  //   // 3.1  Set loca_depth and page_id of buckets which share a page with bucket_idx
  //   // two directions

  //   for (uint32_t i = bucket_idx; i < dir_page->Size(); i += diff) {
  //     dir_page->SetLocalDepth(i, now_local_depth);
  //     dir_page->SetBucketPageId(i, bucket_page_id);
  //   }
  //   for (uint32_t i = bucket_idx; i < dir_page->Size(); i -= diff) {
  //     dir_page->SetLocalDepth(i, now_local_depth);
  //     dir_page->SetBucketPageId(i, bucket_page_id);
  //   }
  //   // 3.2  Set loca_depth and page_id of buckets which share a page with bucket_idx
  //   // two directions
  //   for (uint32_t i = new_bucket_idx; i < dir_page->Size(); i += diff) {
  //     dir_page->SetLocalDepth(i, now_local_depth);
  //     dir_page->SetBucketPageId(i, new_bucket_page_id);
  //   }
  //   for (uint32_t i = new_bucket_idx; i < dir_page->Size(); i -= diff) {
  //     dir_page->SetLocalDepth(i, now_local_depth);
  //     dir_page->SetBucketPageId(i, new_bucket_page_id);
  //   }

  // 4. Rehash and insert
  size_t count = bucket_page->NumReadable();        // count the mappings
  MappingType *data = bucket_page->GetArrayCopy();  // copy data from page
  bucket_page->Reset();                             // clear data.
  new_bucket_page->Reset();
  // now bucket_page and new_bucket_page are cleared.
  for (size_t i = 0; i < count; i++) {
    uint32_t should_in_idx = KeyToDirectoryIndex(data[i].first, dir_page);
    assert(should_in_idx == bucket_idx || should_in_idx == new_bucket_idx);
    if (should_in_idx == bucket_idx) {
      // put in old bucket
      bucket_page->Insert(data[i].first, data[i].second, comparator_);
    } else {
      // put new bucket page
      new_bucket_page->Insert(data[i].first, data[i].second, comparator_);
    }
  }
  // should insert here?
  page_id_t should_be_in = KeyToPageId(key, dir_page);
  assert(should_be_in == bucket_page_id || should_be_in == new_bucket_page_id);
  bool res;
  if (should_be_in == bucket_page_id) {
    res = bucket_page->Insert(key, value, comparator_);
  } else {
    res = new_bucket_page->Insert(key, value, comparator_);
  }

  reinterpret_cast<Page *>(bucket_page)->WUnlatch();
  reinterpret_cast<Page *>(new_bucket_page)->WUnlatch();

  // 5.  Unpin pages
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(new_bucket_page_id, true));

  table_latch_.WUnlock();
  // if it still does not has space, it will recurse. Insert -> SplitInsert -> Insert ...
  //   return Insert(transaction, key, value);
  return res;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->WLatch();
  bool res = bucket_page->Remove(key, value, comparator_);
  // If bucket_page' size == 0, need to check if can merge(which requeire spit_image_page's size == 0 too).
  uint32_t bucket_size = bucket_page->NumReadable();
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  table_latch_.RUnlock();
  if (res && bucket_size == 0) {
    Merge(transaction, key, value);
  }
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  /*
   * Three conditions should not merge:
   * There are three conditions under which we skip the merge:
   * 1. The bucket is no longer empty.
   * 2. The bucket has local depth 0.
   * 3. The bucket's local depth doesn't match its split image's local depth.
   */
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  // 1.  Cal bucket_idx and local_depth, judge and then Load bucket_page
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t bucket_ld = dir_page->GetLocalDepth(bucket_idx);
  // 1.1 condition 2: local_depth > 0.
  if (bucket_ld == 0) {
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.WUnlock();
    return;
  }
  // Only bucket's local_depth > 0, we can calculate split_image_idx.
  uint32_t split_idx = dir_page->GetSplitImageIndex(bucket_idx);
  uint32_t split_ld = dir_page->GetLocalDepth(split_idx);

  // 1.2 condition 3.
  if (split_ld != bucket_ld) {
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.WUnlock();
    return;
  }

  // 1.3  condition 1.
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  //   reinterpret_cast<Page *>(bucket_page)->RLatch();
  if (!bucket_page->IsEmpty()) {
    // reinterpret_cast<Page *>(bucket_page)->RUnlatch();
    table_latch_.WUnlock();
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    return;
  }

  // 2.  Set local_depth and page pointers which point bucket_page or split_page originally.
  // or action as Insert
  page_id_t split_page_id = dir_page->GetBucketPageId(split_idx);
  for (uint32_t i = 0; i > dir_page->Size(); i++) {
    page_id_t pid = dir_page->GetBucketPageId(i);
    if (pid == bucket_page_id || pid == split_page_id) {
      dir_page->SetBucketPageId(i, split_page_id);
      dir_page->SetLocalDepth(i, split_ld - 1);  // or dir_page->DecrLocalDepth(i)
    }
  }

  // 3. Delete bucket_page, before that, you should unpin page
  //   reinterpret_cast<Page *>(bucket_page)->RUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  buffer_pool_manager_->DeletePage(bucket_page_id);  // 这里不应该assert的，可能别的线程想要读取呢 就删不掉了

  // 4.  After merge, check if can shrink.
  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  // 5.  Unpin page
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  table_latch_.WUnlock();
}

// 合并可能存在的另一半空桶  例如 00 10 指向空桶 01 指向非空 11变成空桶  再将11 和 01合并后再合并00 10对应的空桶
// 额外的合并操作
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::ExtraMerge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t index = KeyToDirectoryIndex(key, dir_page);
  uint32_t local_depth = dir_page->GetLocalDepth(index);
  uint32_t dir_size = dir_page->Size();
  bool extra_merge_occur = false;
  if (local_depth > 0) {
    auto extra_bucket_idx = dir_page->GetSplitImageIndex(index);  // 计算合并完后对应桶
    auto extra_local_depth = dir_page->GetLocalDepth(extra_bucket_idx);
    auto extra_bucket_page_id = dir_page->GetBucketPageId(extra_bucket_idx);
    auto *extra_bucket = FetchBucketPage(extra_bucket_page_id);
    if (extra_local_depth == local_depth && extra_bucket->IsEmpty()) {
      extra_merge_occur = true;
      page_id_t tmp_bucket_page_id;
      for (uint32_t i = 0; i < dir_size; i++) {
        tmp_bucket_page_id = dir_page->GetBucketPageId(i);
        if (tmp_bucket_page_id == extra_bucket_page_id) {  // 如果是空桶，更改指向并将深度减一
          dir_page->SetBucketPageId(i, bucket_page_id);
          dir_page->DecrLocalDepth(i);
        } else if (tmp_bucket_page_id == bucket_page_id) {  // 原先桶只需将深度减一
          dir_page->DecrLocalDepth(i);
        }
      }
      buffer_pool_manager_->UnpinPage(extra_bucket_page_id, false, nullptr);  // 先unpin再删除
      buffer_pool_manager_->DeletePage(extra_bucket_page_id, nullptr);
      bool ret = dir_page->CanShrink();
      if (ret) {  // 降低全局深度
        dir_page->DecrGlobalDepth();
      }
    }
    if (!extra_merge_occur) {  // 额外的合并未发生
      buffer_pool_manager_->UnpinPage(extra_bucket_page_id, false, nullptr);
    }
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  table_latch_.WUnlock();
  return extra_merge_occur;
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub