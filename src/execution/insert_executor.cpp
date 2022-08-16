/*
 * @Author: lxk
 * @Date: 2022-08-03 21:13:14
 * @LastEditors: lxk
 * @LastEditTime: 2022-08-14 09:40:55
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {  // 需要再次转移所有权 或者swap也行
  table_oid_t table_id = plan_->TableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(table_id);
}

void InsertExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

void InsertExecutor::InsertTuple(Tuple *tuple) {
  Transaction *transaction = exec_ctx_->GetTransaction();
  RID rid;
  table_info_->table_->InsertTuple(*tuple, &rid, transaction);
  // 3.  If there are indexes, insert into indexes
  std::vector<bustub::IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (auto index : indexes) {
    Tuple index_tmp_tuple = tuple->KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    // insertEntry(key, value, transaction) like hash_table.insert(key, value, trx)
    index->index_->InsertEntry(index_tmp_tuple, rid, transaction);
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // Maybe unused, because the insert operator is the root operator in plan tree
  // No need to return something by the tuple and rid pointer.
  // First.  Init some environemnt variable.
  Schema table_schema = table_info_->schema_;
  // Secoind. Judge which type insert (raw insert or child operator)
  if (plan_->IsRawInsert()) {
    std::vector<std::vector<bustub::Value>> values = plan_->RawValues();
    for (const auto &value : values) {
      // 1.  Construct tuple to insert
      Tuple tmp_tuple(value, &table_info_->schema_);
      // 2.  Insert this tuple
      try {
        InsertTuple(&tmp_tuple);
      } catch (Exception &e) {
        throw Exception(ExceptionType::UNKNOWN_TYPE, "InsertExecutor: row insert error!");
      }
    }
  } else {
    Tuple tmp_tuple;
    RID tmp_rid;
    child_executor_->Init();  // Init() executed in father executor.
    try {
      while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
        InsertTuple(&tmp_tuple);
      }
    } catch (std::exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "InsertExecutor: child executor insert error!");
    }
  }
  return false;
}

}  // namespace bustub
