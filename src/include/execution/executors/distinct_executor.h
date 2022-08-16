/*
 * @Author: lxk
 * @Date: 2022-08-03 21:13:14
 * @LastEditors: lxk
 * @LastEditTime: 2022-08-13 21:42:31
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {
struct DistinctKey {
  std::vector<Value> distincts_;

  // override the operator==
  bool operator==(const DistinctKey &other) const {
    for (uint32_t i = 0; i < other.distincts_.size(); i++) {
      if (distincts_[i].CompareEquals(other.distincts_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

// 注意 特化函数一点都不能少 包括后边的const或者noexcept这些
namespace std {
template <>
struct hash<bustub::DistinctKey> {
  std::size_t operator()(const bustub::DistinctKey &key) const {
    size_t cur_hash = 0;
    for (const auto &k : key.distincts_) {
      if (!k.IsNull()) {
        cur_hash = bustub::HashUtil::CombineHashes(cur_hash, bustub::HashUtil::HashValue(&k));
      }
    }
    return cur_hash;
  }
};
}  // namespace std

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  // 计算结果完全存储 Next中使用iter遍历
  std::unordered_map<DistinctKey, Tuple> map_;
  std::unordered_map<DistinctKey, Tuple>::iterator iter_;
};
}  // namespace bustub
