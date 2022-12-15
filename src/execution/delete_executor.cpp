/*
 * @Author: lxk
 * @Date: 2022-08-03 21:13:14
 * @LastEditors: lxk
 * @LastEditTime: 2022-08-21 10:52:23
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->TableOid());
}

void DeleteExecutor::Init() {
  assert(child_executor_ != nullptr);
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  std::vector<bustub::IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Transaction *transaction = exec_ctx_->GetTransaction();
  LockManager *lockmanager = exec_ctx_->GetLockManager();
  Catalog *catalog = exec_ctx_->GetCatalog();

  RID tmp_rid;
  Tuple tmp_tuple;
  try {
    while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
      if (transaction->IsSharedLocked(tmp_rid)) {
        lockmanager->LockUpgrade(transaction, tmp_rid);  // 之前查询获取了读锁，现还没有解锁，在需要将锁升级
      } else {
        lockmanager->LockExclusive(transaction, tmp_rid);  // 未获取读锁
      }

      // Mark delete, and ApplyDelete when commit or Rollbackdelete
      table_info_->table_->MarkDelete(tmp_rid, exec_ctx_->GetTransaction());
      for (auto index : indexes) {
        // Calculate and Delete old index entry
        Tuple index_tmp_tuple =
            tmp_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(index_tmp_tuple, tmp_rid, transaction);
        // == Add index write set.
        transaction->AppendTableWriteRecord(
            IndexWriteRecord{tmp_rid, table_info_->oid_, WType::DELETE, tmp_tuple, index->index_oid_, catalog});
      }
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "DeleteExecutor: delete error");
  }
  return false;
}

}  // namespace bustub
