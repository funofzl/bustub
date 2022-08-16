//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  // Init the left executor and right executor
  left_executor_->Init();
  right_executor_->Init();
  RID rid;
  non_empty_ = left_executor_->Next(&left_tuple_, &rid);  // for the Next be called Firstly.
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  RID left_rid;
  RID right_rid;
  Tuple right_tuple;
  const Schema *left_schema = left_executor_->GetOutputSchema();
  const Schema *right_schema = right_executor_->GetOutputSchema();
  const Schema *output_schema = GetOutputSchema();

  const AbstractExpression *predicate = plan_->Predicate();
  if (!non_empty_) {
    return false;
  }
  while (true) {
    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      if (!(non_empty_ = left_executor_->Next(&left_tuple_, &left_rid))) {
        return false;
      }
      right_executor_->Init();  // Right table's Iter reset to the Begin()
      if (!right_executor_->Next(&right_tuple, &right_rid)) {
        return false;  // right table doesn't has tuples
      }
    }
    /** 不同的expresion的Evaluate和EvaluateJoin的作用不同:
     *  Column.Expr代表提取值(rturn Value)
     *  Predicate代表是否符合条件(return bool) */
    if (predicate == nullptr ||
        predicate->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
      // construct the output schema and put them into tuple
      std::vector<Value> vals;
      for (size_t i = 0; i < output_schema->GetColumnCount(); i++) {
        vals.push_back(
            output_schema->GetColumn(i).GetExpr()->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema));
      }
      *tuple = Tuple(vals, output_schema);
      // When join, rid is noused
      return true;
    }
  }
  return false;
}  // namespace bustub

}  // namespace bustub
