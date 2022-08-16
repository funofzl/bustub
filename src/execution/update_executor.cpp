/*
 * @Author: lxk
 * @Date: 2022-08-03 21:13:14
 * @LastEditors: lxk
 * @LastEditTime: 2022-08-14 09:41:44
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

void UpdateExecutor::Init() {
  assert(child_executor_ != nullptr);
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  std::vector<bustub::IndexInfo *> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Transaction *transaction = exec_ctx_->GetTransaction();

  child_executor_->Init();
  RID tmp_rid;
  Tuple tmp_tuple;
  Tuple new_tuple;
  try {
    while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
      new_tuple = GenerateUpdatedTuple(tmp_tuple);
      table_info_->table_->UpdateTuple(new_tuple, tmp_rid, exec_ctx_->GetTransaction());
      for (auto index : indexes) {
        // Calculate and Delete old index entry
        Tuple index_tmp_tuple =
            tmp_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(index_tmp_tuple, tmp_rid, transaction);
        // Calculate and Insert new index entry
        Tuple index_new_tuple =
            new_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(index_new_tuple, tmp_rid, transaction);
      }
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "UpdateExecutor: update error");
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
