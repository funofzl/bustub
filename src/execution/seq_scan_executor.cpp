/*
 * @Author: lxk
 * @Date: 2022-08-03 21:13:14
 * @LastEditors: lxk
 * @LastEditTime: 2022-08-21 09:59:43
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

void SeqScanExecutor::Init() {
  iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
  // 可重复读：给所有元组加上读锁，事务提交后再解锁
  auto transaction = exec_ctx_->GetTransaction();
  auto lockmanager = exec_ctx_->GetLockManager();
  if (transaction->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    auto iter = table_info_->table_->Begin(exec_ctx_->GetTransaction());
    while (iter != table_info_->table_->End()) {
      lockmanager->LockShared(transaction, iter->GetRid());
      ++iter;
    }
  }
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto transaction = exec_ctx_->GetTransaction();
  auto lockmanager = exec_ctx_->GetLockManager();
  while (iter_ != table_info_->table_->End()) {
    // RC and RR need to lock. if has been locked, no effect.
    if (transaction->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        !transaction->IsExclusiveLocked(iter_->GetRid()) && !transaction->IsSharedLocked(iter_->GetRid())) {
      lockmanager->LockShared(transaction, iter_->GetRid());
    }

    RID tmp_rid = iter_->GetRid();
    // 1.  Evaluate every column value in output_schema's columns.
    //     If just get value, then return value, otherwise get the value calculated(e.g aggregate).
    const Schema *output_schema = plan_->OutputSchema();
    std::vector<Value> vals;
    vals.reserve(output_schema->GetColumnCount());
    for (size_t i = 0; i < output_schema->GetColumnCount(); i++) {
      vals.push_back(output_schema->GetColumn(i).GetExpr()->Evaluate(&(*iter_), &(table_info_->schema_)));
    }
    // == If is RC, and is read lock, we can release lock.
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
        transaction->IsSharedLocked(iter_->GetRid())) {
      lockmanager->Unlock(transaction, iter_->GetRid());
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
