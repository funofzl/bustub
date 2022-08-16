/*
 * @Author: lxk
 * @Date: 2022-08-03 21:13:14
 * @LastEditors: lxk
 * @LastEditTime: 2022-08-14 11:05:04
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

// 根据AggregteKey的做法，定义一个JoinKey并定义一个Hash的特化版本
namespace bustub {

/** Implements JoinKey and JoinKeyComparator */
struct HashJoinKey {
  Value join_key_;
  /**
   * Compares two HashJoin keys for equality.
   * @param other the other hashjoin key to be compared with
   * @return `true` if both hashjoin keys have equivalent group-by expressions, `false` otherwise
   */
  bool operator==(const HashJoinKey &other) const {
    return join_key_.CompareEquals(other.join_key_) == CmpBool::CmpTrue;
  }
  bool operator<(const HashJoinKey &other) const {
    return join_key_.CompareLessThan(other.join_key_) == CmpBool::CmpTrue;
  }
};

};  // namespace bustub

namespace std {

/** Implements std::hash on JoinKey */
template <>
struct hash<bustub::HashJoinKey> {
  std::size_t operator()(const bustub::HashJoinKey &hash_join_key) const {
    size_t curr_hash = bustub::HashUtil::HashValue(&(hash_join_key.join_key_));
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  // left child
  std::unique_ptr<AbstractExecutor> left_executor_;
  // right child
  std::unique_ptr<AbstractExecutor> right_executor_;
  // Hash table
  std::map<HashJoinKey, std::vector<Tuple>> hash_table_;
  // the right tuple being processed.
  Tuple right_tuple_;
  // the indice of the left tuple being join with the right_tuple_
  size_t indice_;
  // non empty flag
  bool non_empty_;
};

}  // namespace bustub
