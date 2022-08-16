/*
 * @Author: lxk
 * @Date: 2022-08-03 21:13:14
 * @LastEditors: lxk
 * @LastEditTime: 2022-08-13 21:16:05
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iter_(nullptr, RID(), nullptr) {
  table_oid_t table_id = plan_->GetTableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(table_id);
}

void SeqScanExecutor::Init() { iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != table_info_->table_->End()) {
    RID tmp_rid = iter_->GetRid();
    // 1.  Evaluate every column value in output_schema's columns.
    //     If just get value, then return value, otherwise get the value calculated(e.g aggregate).
    const Schema *output_schema = plan_->OutputSchema();
    std::vector<Value> vals;
    vals.reserve(output_schema->GetColumnCount());
    for (size_t i = 0; i < output_schema->GetColumnCount(); i++) {
      vals.push_back(output_schema->GetColumn(i).GetExpr()->Evaluate(&(*iter_), &(table_info_->schema_)));
    }
    iter_++;
    // 2.  Evalueta the predicate
    Tuple tmp_tuple(vals, output_schema);
    const AbstractExpression *predicate = plan_->GetPredicate();
    if (predicate == nullptr || predicate->Evaluate(&tmp_tuple, output_schema).GetAs<bool>()) {
      *rid = tmp_rid;
      *tuple = tmp_tuple;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
