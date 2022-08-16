/*
 * @Author: lxk
 * @Date: 2022-08-03 21:13:14
 * @LastEditors: lxk
 * @LastEditTime: 2022-08-13 19:07:28
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  // Build Hash Table
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    DistinctKey dis_keys;
    // for (auto &col : GetOutputSchema()->GetColumns()) {
    //   dis_keys.distincts_.push_back(col.GetExpr()->Evaluate(&tuple, child_executor_->GetOutputSchema()));
    // }
    for (uint32_t idx = 0; idx < plan_->OutputSchema()->GetColumnCount(); idx++) {
      dis_keys.distincts_.push_back(tuple.GetValue(plan_->OutputSchema(), idx));
    }

    if (map_.count(dis_keys) == 0) {
      map_.insert({dis_keys, tuple});
    }
  }
  iter_ = map_.begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ == map_.end()) {
    return false;
  }
  *tuple = iter_->second;
  ++iter_;
  // No RID
  return true;
}

}  // namespace bustub
