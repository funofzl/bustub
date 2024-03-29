
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
  auto dir_page = CreateDirectoryPage(&directory_page_id_);  // 创建目录页

  page_id_t bucket_page_id;
  buffer_pool_manager_->NewPage(&bucket_page_id, nullptr);  // 申请第一个桶的页
  dir_page->SetBucketPageId(0, bucket_page_id);

  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);     // 放回桶页
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);  // 放回目录页
}

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
  uint32_t index = Hash(key) & dir_page->GetGlobalDepthMask();
  return index;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t index = KeyToDirectoryIndex(key, dir_page);
  page_id_t page_id = dir_page->GetBucketPageId(index);
  return page_id;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::CreateDirectoryPage(page_id_t *bucket_page_id) {
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->NewPage(&directory_page_id_, nullptr)->GetData());  // 创建目录页
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::CreateBucketPage(page_id_t *bucket_page_id) {
  auto *new_bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(bucket_page_id, nullptr)->GetData());
  return new_bucket_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  auto directory_page = reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->FetchPage(directory_page_id_, nullptr)->GetData());
  return directory_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  auto bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id, nullptr)->GetData());
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto *bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->RLatch();
  bool ret = bucket_page->GetValue(key, comparator_, result);  // 读取桶页内容前加页的读锁
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();

  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();
  return ret;
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

// 自己的
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
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
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
  //  4.1 First method, set all bucket whose page_id pointer to bucket_page_id
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
  // 4.2  Second method: calculate the diff and set the bucket which dist bucket_idx k*diff.
  //   uint32_t diff = 0x1 << now_local_depth;  // the distance of two buckets which share a page
  //   // 3.1  Set loca_depth and page_id of buckets which share a page with bucket_idx
  //   // two directions
  //   uint32_t start_idx1 = bucket_idx % diff;
  //   for (uint32_t i = start_idx1; i < dir_page->Size(); i += diff) {
  //     dir_page->SetLocalDepth(i, now_local_depth);
  //     dir_page->SetBucketPageId(i, bucket_page_id);
  //   }
  //   // 3.2  Set loca_depth and page_id of buckets which share a page with bucket_idx
  //   // two directions
  //   uint32_t start_idx2 = new_bucket_idx % diff;
  //   for (uint32_t i = start_idx2; i < dir_page->Size(); i += diff) {
  //     dir_page->SetLocalDepth(i, now_local_depth);
  //     dir_page->SetBucketPageId(i, new_bucket_page_id);
  //   }

  // 5. Rehash and insert
  //   LOG_DEBUG("bucket_idx: %d -- new_bucket_idx: %d", bucket_idx, new_bucket_idx);
  //   dir_page->PrintDirectory();
  KeyType bucket_key;
  ValueType bucket_value;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!bucket_page->IsReadable(i)) {
      continue;
    }
    bucket_key = bucket_page->KeyAt(i);
    bucket_value = bucket_page->ValueAt(i);
    uint32_t should_in_idx = KeyToDirectoryIndex(bucket_key, dir_page);
    assert(should_in_idx == bucket_idx || should_in_idx == new_bucket_idx);
    if (should_in_idx == new_bucket_idx) {
      // delete from old_bucket
      bucket_page->RemoveAt(i);
      // put new bucket page
      new_bucket_page->Insert(bucket_key, bucket_value, comparator_);
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

  // 6.  Unpin pages
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
  if (bucket_size == 0) {  // through remove return false, if it is empty, we can merge it.
    Merge(transaction, key, value);
    // while (ExtraMerge(transaction, key, value)) {
    // }
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
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t bucket_ld = dir_page->GetLocalDepth(bucket_idx);

  // 1.    Cal bucket_idx and local_depth, judge and then Load bucket_page
  // 1.1   condition 2: local_depth > 0.
  if (bucket_ld == 0) {
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.WUnlock();
    return;
  }
  // 1.2   condition 3: split_local_depth == bucket_local_depth
  uint32_t split_idx = dir_page->GetSplitImageIndex(bucket_idx);
  uint32_t split_ld = dir_page->GetLocalDepth(split_idx);
  if (split_ld != bucket_ld) {
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    table_latch_.WUnlock();
    return;
  }
  // 1.3   condition 1: Empty() == true
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  if (!bucket_page->IsEmpty()) {
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    table_latch_.WUnlock();
    return;
  }

  // 2.    Set local_depth and page pointers which point bucket_page or split_page originally.
  page_id_t split_page_id = dir_page->GetBucketPageId(split_idx);
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    page_id_t pid = dir_page->GetBucketPageId(i);
    if (pid == bucket_page_id || pid == split_page_id) {
      dir_page->SetBucketPageId(i, split_page_id);
      dir_page->SetLocalDepth(i, split_ld - 1);  // or dir_page->DecrLocalDepth(i)
    }
  }

  // 3. Delete bucket_page, before that, you should unpin page
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

// 测试方法
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintDir() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t dir_size = dir_page->Size();

  dir_page->PrintDirectory();
  printf("dir size is: %d\n", dir_size);
  for (uint32_t idx = 0; idx < dir_size; idx++) {
    auto bucket_page_id = dir_page->GetBucketPageId(idx);
    HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
    bucket_page->PrintBucket();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  }

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::RemoveAllItem(Transaction *transaction, uint32_t bucket_idx) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  auto bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  auto items = bucket_page->GetAllItem();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  table_latch_.RUnlock();
  for (auto &item : items) {
    Remove(nullptr, item.first, item.second);
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
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
