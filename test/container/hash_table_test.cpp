//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// NOLINTNEXTLINE

// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  ht.VerifyIntegrity();

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  ht.VerifyIntegrity();

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, Sample2Test) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  for (int i = 1; i <= 496; i++) {
    ht.Insert(nullptr, i, i);
  }

  for (int i = 497; i <= 1000; i++) {
    LOG_DEBUG("insert: %d", i);
    ht.Insert(nullptr, i, i);
  }
  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

/* 只测试两个桶 */
TEST(HashTableTest, MySplitShrinkTest1) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  printf("block size is: %ld\n", (4 * PAGE_SIZE / (4 * sizeof(std::pair<int, int>) + 1)));  // 桶容量
  // 插入496个值
  for (int i = 0; i < 496; i++) {
    ht.Insert(nullptr, i, i);
  }
  ht.PrintDir();                 // 输出目录信息
  ht.Insert(nullptr, 496, 496);  // 桶分裂
                                 //   EXPECT_EQ(bpm->, 0);  // 无论何时，被占用的页都应该为0
  EXPECT_EQ(ht.GetGlobalDepth(), 1);
  ht.PrintDir();
  ht.RemoveAllItem(nullptr, 1);  // 删除索引1对应的桶的所有项
                                 //   EXPECT_EQ(bpm->GetOccupiedPageNum(), 0);
  ht.PrintDir();
  ht.RemoveAllItem(nullptr, 0);  // 删除索引0对应的桶的所有项
                                 //   EXPECT_EQ(bpm->GetOccupiedPageNum(), 0);
  ht.PrintDir();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

/* 测试4个桶 */
TEST(HashTableTest, MySplitShrinkTest2) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  printf("block size is: %ld\n", (4 * PAGE_SIZE / (4 * sizeof(std::pair<int, int>) + 1)));
  // 插入1000个值
  for (int i = 0; i < 1500; i++) {
    ht.Insert(nullptr, i, i);
  }
  ht.PrintDir();
  LOG_DEBUG("begin...........................................................................");
  ht.RemoveAllItem(nullptr, 0);
  //   EXPECT_EQ(bpm->GetOccupiedPageNum(), 0);  // 删除索引0对应的桶的所有项
  LOG_DEBUG("after remove bucket 0's page...........................................................................");
  ht.PrintDir();
  ht.RemoveAllItem(nullptr, 0);
  //   EXPECT_EQ(bpm->GetOccupiedPageNum(), 0);  // 再次删除索引0对应的桶的所有项
  //   bpm->PrintExistPageId();                  // 输出缓冲池中页的状态
  LOG_DEBUG("after remove2 bucket 0...........................................................................");
  ht.PrintDir();
  ht.RemoveAllItem(nullptr, 1);  // 删除索引1对应的桶的所有项，应该发生递归合并
  LOG_DEBUG("should has two big page or a big page");
  ht.PrintDir();
  //   EXPECT_EQ(ht.GetGlobalDepth(), 0);
  //   EXPECT_EQ(bpm->GetOccupiedPageNum(), 0);

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
