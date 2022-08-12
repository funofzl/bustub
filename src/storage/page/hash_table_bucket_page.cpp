//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

/************ Helpers ************/

// inline static bool GetNthBit(const char *char_arr, uint32_t bucket_ind) {
//   uint32_t arr_index = bucket_ind / 8;
//   uint32_t bit_offset = bucket_ind % 8;
//   return (char_arr[arr_index] & (static_cast<char>(1) << bit_offset)) != 0;
// }

// inline static void SetNthBit(char *char_arr, uint32_t bucket_ind) {
//   uint32_t arr_index = bucket_ind / 8;
//   uint32_t bit_offset = bucket_ind % 8;
//   char_arr[arr_index] |= (static_cast<char>(1) << bit_offset);
// }

// inline static void UnSetNthBit(char *char_arr, uint32_t bucket_ind) {
//   uint32_t arr_index = bucket_ind / 8;
//   uint32_t bit_offset = bucket_ind % 8;
//   char_arr[arr_index] &= (~(static_cast<char>(1) << bit_offset));
// }

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetUnreadable(uint32_t bucket_idx) {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  readable_[index] &= ~(1 << offset);
}

// ======================================================================

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool flag = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsReadable(i) && cmp(array_[i].first, key) == 0) {
      result->push_back(array_[i].second);
      flag = true;
    }
    if (!IsOccupied(i)) {
      break;
    }
  }
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  uint32_t to_insert = BUCKET_ARRAY_SIZE;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      // judge if equal
      if (cmp(array_[i].first, key) == 0 && ValueAt(i) == value) {
        return false;
      }
    } else {
      if (to_insert == BUCKET_ARRAY_SIZE) {
        to_insert = i;
      }
      if (!IsOccupied(i)) {
        break;
      }
    }
  }
  // judge if fulled which means no space to insert
  if (to_insert == BUCKET_ARRAY_SIZE) {
    return false;
  }
  array_[to_insert] = {key, value};
  SetOccupied(to_insert);
  SetReadable(to_insert);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  // find the pair and remove it.
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && cmp(KeyAt(i), key) == 0 && value == ValueAt(i)) {
      SetUnreadable(i);
      return true;  // No duplicate
    }
    if (!IsOccupied(i)) {
      break;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  if (IsReadable(bucket_idx)) {
    return array_[bucket_idx].first;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  if (IsReadable(bucket_idx)) {
    return array_[bucket_idx].second;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  SetUnreadable(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  return static_cast<bool>(occupied_[index] & (1 << offset));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  occupied_[index] |= 1 << offset;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  return static_cast<bool>(readable_[index] & (1 << offset));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  readable_[index] |= 1 << offset;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  uint32_t complete_byte_count = BUCKET_ARRAY_SIZE / 8;
  for (uint32_t i = 0; i < complete_byte_count; i++) {
    if (readable_[i] != static_cast<char>(0xff)) {
      return false;
    }
  }
  uint32_t rest = BUCKET_ARRAY_SIZE - complete_byte_count * 8;
  return !(rest != 0 && readable_[complete_byte_count] != static_cast<char>((0x1 << rest) - 1));
  //   return !(rest != 0 && readable_[(BUCKET_ARRAY_SIZE - 1) / 8] != static_cast<char>((1 << rest) - 1));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t cnt = 0;
  uint32_t read_array_size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;

  uint8_t n;
  for (uint32_t i = 0; i < read_array_size; i++) {
    n = static_cast<uint8_t>(readable_[i]);
    while (n != 0) {
      n &= (n - 1);  // 将最低位1清0, 计算1的个数
      cnt++;
    }
  }
  return cnt;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  uint32_t read_array_size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  for (uint32_t i = 0; i < read_array_size; i++) {
    if (readable_[i] != static_cast<char>(0)) {  // 不能直接与0比较
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::Reset() {
  memset(occupied_, 0, sizeof(occupied_));
  memset(readable_, 0, sizeof(readable_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
