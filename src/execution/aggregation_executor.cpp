//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  // pipeline breaker: need to build aggregate hash table
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    AggregateKey agg_key = MakeAggregateKey(&tuple);      // for(col : plan_->Groups) col->expr->Evaluate
    AggregateValue agg_val = MakeAggregateValue(&tuple);  // for(col : plan_->Aggres) col->expr->Evaluate
    aht_.InsertCombine(agg_key, agg_val);
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  AggregateKey agg_key;
  AggregateValue agg_val;
  while (aht_iterator_ != aht_.End()) {
    agg_key = aht_iterator_.Key();
    agg_val = aht_iterator_.Val();
    ++aht_iterator_;
    const AbstractExpression *having = plan_->GetHaving();
    if (having == nullptr || having->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_).GetAs<bool>()) {
      std::vector<Value> vals;
      for (auto &col : plan_->OutputSchema()->GetColumns()) {
        vals.push_back(col.GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_val.aggregates_));
      }
      *tuple = Tuple(vals, GetOutputSchema());
      // No RID
      return true;
    }
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
