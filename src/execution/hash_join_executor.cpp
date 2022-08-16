/*
 * @Author: lxk
 * @Date: 2022-08-03 21:13:14
 * @LastEditors: lxk
 * @LastEditTime: 2022-08-14 12:02:00
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  // Build Hash Tbale for outer/left table
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    HashJoinKey join_key;
    join_key.join_key_ = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_executor_->GetOutputSchema());
    // 重复的数据要额外保存
    if (hash_table_.count(join_key) != 0) {
      hash_table_[join_key].emplace_back(tuple);
    } else {
      hash_table_[join_key] = std::vector{tuple};
    }
  }
  RID right_rid;
  non_empty_ = right_executor_->Next(&right_tuple_, &right_rid);
  indice_ = 0;
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (hash_table_.empty()) {
    return false;
  }
  if (!non_empty_) {
    return false;
  }
  RID right_rid;
  HashJoinKey join_key;
  while (true) {
    join_key.join_key_ = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, right_executor_->GetOutputSchema());

    // 1.  Only hash_table[join_key] exists and the entry has not been processedfinished, we can join and return.
    if (hash_table_.count(join_key) != 0) {
      std::vector<bustub::Tuple> left_tuples = hash_table_.find(join_key)->second;
      if (indice_ < left_tuples.size()) {
        Tuple left_tuple = left_tuples[indice_++];
        std::vector<Value> output;
        for (const auto &col : GetOutputSchema()->GetColumns()) {
          output.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple_,
                                                       right_executor_->GetOutputSchema()));
        }
        *tuple = Tuple(output, GetOutputSchema());
        // No RID
        return true;
      }
    }
    // 2.  Otherwise, current right_tuple_'s join proceseing finished, now change to next
    if (!(non_empty_ = right_executor_->Next(&right_tuple_, &right_rid))) {
      return false;
    }
    indice_ = 0;  // from first to join
  }
  return false;
}

}  // namespace bustub
