//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

#include <cassert>  // NOLINT
#include <utility>  // NOLINT
#include <vector>   // NOLINT

namespace bustub {
/**
 * @brief When there is no a txn holds the lock, we can grant
 *
 */
void LockManager::GrantLock(const RID &rid) {
  auto iter = lock_table_.find(rid);
  assert(iter != lock_table_.end());
  auto it = iter->second.request_queue_.begin();
  bool has_read_lock = false;
  while (it != iter->second.request_queue_.end()) {
    // If exists a write lock, we can not grant a lock to other txns.
    if (it->lock_mode_ == LockMode::EXCLUSIVE) {
      if (!has_read_lock) {
        it->granted_ = true;
      }
      break;
    }
    it->granted_ = true;
    has_read_lock = true;
    it++;
  }
  iter->second.cv_.notify_all();
}

/**
 *  If the lock procedure(SHRINKING/GROWING) should has a check, if should, it will not able to lock.
 *
 *                   | Read Lock    | Write Lock |
 *  READ_UNCOMMITTED |  should      |            |
 *  READ_COMMITTED   |              |   should   |
 *  REPEATABLE_READ  |  should      |   should   |
 */

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  std::unique_lock<std::mutex> lock(latch_);
  auto iter = lock_table_.find(rid);

  // 1.   Check.
  // 1.1  If isolation level == READ_UNCOMMITTED, don't need the shared lock.
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 1.2.  If isolation level == REPEATABLE_READ, Shrinking state can not get lock.
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 1.3.  Detect if holds the lock.
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  // 2.   If queue does not exists, create it.
  if (iter == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
    iter = lock_table_.find(rid);
  }

  // 3.   Remove the txns in queue which is newer than current txn.
  for (auto it = iter->second.request_queue_.begin(); it != iter->second.request_queue_.end();) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      return false;  // Can not get the lock more than one time.
    }
    if (it->lock_mode_ == LockMode::EXCLUSIVE && it->txn_id_ > txn->GetTransactionId()) {
      // Remove entry from queue directly, when Abort call the UnLock, we return false directly.
      Transaction *trans = TransactionManager::GetTransaction(it->txn_id_);
      if (it->granted_) {
        trans->GetSharedLockSet()->erase(rid);
        trans->GetExclusiveLockSet()->erase(rid);
      }
      iter->second.request_queue_.erase(it++);
      trans->SetState(TransactionState::ABORTED);
    } else {
      it++;
    }
  }
  iter->second.cv_.notify_all();

  // 4.   Insert the LockReuqest into queue.
  LockRequest tmp = LockRequest{txn->GetTransactionId(), LockMode::SHARED};
  tmp.granted_ = false;
  iter->second.request_queue_.push_back(tmp);
  LockRequest &lock_req = iter->second.request_queue_.back();

  // 5.   Grant Lock.
  GrantLock(rid);
  //   auto judge_func = [&]() {
  //     for (const auto &req : iter->second.request_queue_) {
  //       if (txn->GetTransactionId() == req.txn_id_) {
  //         return true;
  //       }
  //       if (req.lock_mode_ != LockMode::SHARED) {
  //         return false;
  //       }
  //     }

  //     return true;
  //   };

  // 6. Waiting if not be granted
  // Onlt two conditions: 1. Being aborted during waiting.  |  2. being granted the lock.
  while (!lock_req.granted_ && txn->GetState() != TransactionState::ABORTED) {
    lock_table_[rid].cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}  // namespace bustub

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  std::unique_lock<std::mutex> lock(latch_);
  auto iter = lock_table_.find(rid);

  // 1.   Check.
  // 1.1  If isolation level == REPEATABLE_READ or READ_COMMITTED, Shrinking state can not get exclusive lock.
  if (txn->GetState() == TransactionState::SHRINKING && (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
                                                         txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 1.2.  Detect if holds the lock.
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  if (txn->IsSharedLocked(rid)) {
    // should through the UpgradeLock function.
    return false;
  }

  // 2.   If queue does not exists, create it.
  if (iter == lock_table_.end()) {
    lock_table_[rid].upgrading_ = INVALID_TXN_ID;
    iter = lock_table_.find(rid);
  }

  // 3.   Remove the txns in queue which is newer than current txn.
  for (auto it = iter->second.request_queue_.begin(); it != iter->second.request_queue_.end();) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      return false;  // Can not get the lock more than one time.
    }
    if (it->txn_id_ > txn->GetTransactionId()) {
      // Remove entry from queue directly, when Abort call the UnLock, we return false directly.
      Transaction *trans = TransactionManager::GetTransaction(it->txn_id_);
      if (it->granted_) {
        trans->GetSharedLockSet()->erase(rid);
        trans->GetExclusiveLockSet()->erase(rid);
      }
      iter->second.request_queue_.erase(it++);
      trans->SetState(TransactionState::ABORTED);
    } else {
      it++;
    }
  }
  iter->second.cv_.notify_all();

  // 4.   Insert the LockReuqest into queue.
  LockRequest tmp = LockRequest{txn->GetTransactionId(), LockMode::EXCLUSIVE};
  tmp.granted_ = false;
  iter->second.request_queue_.push_back(tmp);
  LockRequest &lock_req = iter->second.request_queue_.back();

  // 5.  Grant Lock.
  GrantLock(rid);

  // 6. Waiting if not be granted
  while (!lock_req.granted_ && txn->GetState() != TransactionState::ABORTED) {
    lock_table_[rid].cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  //   txn->SetState(TransactionState::GROWING);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // The rid should already locked by the request transaction in share mode, so we can upgrade lcok mode.
  // We need kill other txns which is newer than this txn and for/in write lock.
  // On this occation, perhaps other transactions lock in share mode, so we need wait for unlocking other transactions
  // on this rid, during this process, we should ensure no other transactions lock(in share or exclusive mode) on
  // this rid by check the upgrading member variable.
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  std::unique_lock<std::mutex> lock(latch_);
  auto iter = lock_table_.find(rid);
  assert(iter != lock_table_.end());  // request transaction must lock in share mode on this rid

  // 1.   Check.
  // 1.1  If you have been granted write lock, return true.
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  // 1.2  If you does not have the read lock, return false.
  if (!txn->IsSharedLocked(rid)) {
    return false;
  }
  // 1.3  If another transactions waiting for upgrade lock, Abort this transaction!
  if (iter->second.upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // 2.   Find the txn position and in queue and construct the new LockRequest.
  std::list<LockRequest>::iterator target_pos;
  for (auto it = iter->second.request_queue_.begin(); it != iter->second.request_queue_.end(); it++) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      target_pos = it;
      break;
    }
  }
  LockRequest lock_req = *target_pos;
  lock_req.granted_ = false;
  lock_req.lock_mode_ = LockMode::EXCLUSIVE;

  // 3.  Remove the entry of read lock, and insert the write lock entry.
  iter->second.request_queue_.erase(target_pos);
  txn->GetSharedLockSet()->erase(rid);

  // 4.  Find the last read lock and insert after it.
  std::list<LockRequest>::iterator last_read_pos = iter->second.request_queue_.begin();
  for (auto it = iter->second.request_queue_.begin(); it != iter->second.request_queue_.end(); it++) {
    if (it->lock_mode_ == LockMode::SHARED) {
      last_read_pos = it;
    } else {
      break;
    }
  }

  iter->second.request_queue_.insert(last_read_pos, lock_req);
  //    Set upgrading to txn id, and wait for locking successfully.
  iter->second.upgrading_ = txn->GetTransactionId();

  for (auto it = iter->second.request_queue_.begin(); it != iter->second.request_queue_.end();) {
    if (it->txn_id_ > txn->GetTransactionId()) {
      iter->second.request_queue_.erase(it++);
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->erase(rid);
      txn->SetState(TransactionState::ABORTED);
    } else {
      it++;
    }
  }
  iter->second.cv_.notify_all();

  // 4.   If is the first one, we can grant it.
  if (iter->second.request_queue_.begin()->txn_id_ == txn->GetTransactionId()) {
    iter->second.request_queue_.begin()->granted_ = true;
    txn->GetExclusiveLockSet()->emplace(rid);
    iter->second.upgrading_ = INVALID_TXN_ID;
    return true;
  }

  // Will not change lock status(e.g. add another txn), so we need not to do GrantLock().
  // 5.  Note that, perhaps txn will be aborted during waiting.
  while (txn->GetState() != TransactionState::ABORTED && !last_read_pos->granted_) {
    iter->second.cv_.wait(lock);
  }
  // 6.  If the txn has been aborted, we need to return false;
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  iter->second.upgrading_ = INVALID_TXN_ID;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  auto iter = lock_table_.find(rid);
  if (iter == lock_table_.end()) {
    return false;
  }
  // 1.  Determine if is the only one txn which holds lock.
  //     If the last read lock or write lock, we should grant the first txn write lock in queue or consecutive read lock
  //     from head of queue. Otherwise,we can not grant any txns in queue, for example: if this txn is not the only read
  //     lock, it means we need not grant other txns read lock, because unlock this txn will not bring about any impact.
  //     So, we should determine if current txn is the only one which holds the lock.
  bool only_one = false;
  std::list<LockRequest>::iterator target_pos = iter->second.request_queue_.end();
  for (auto it = iter->second.request_queue_.begin(); it != iter->second.request_queue_.end(); it++) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      assert(it->granted_ == true);
      target_pos = it;
      if (it->lock_mode_ == LockMode::EXCLUSIVE) {
        only_one = true;
        break;
      }                         // also need to
    } else if (it->granted_) {  // If we grant other txns locks, current is not the only one which get lock.
      only_one = false;
    }
  }
  // 2.   If there is no this txn, return false;
  if (target_pos == iter->second.request_queue_.end()) {
    return false;
  }

  // 2.  Delete enty from queue
  //     only write lock or read lock in RR, can weset status to shrinking.
  //     Note that, write lock in RU, we also need to change state to shrinking.
  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->IsExclusiveLocked(rid)) {
      txn->SetState(TransactionState::SHRINKING);
    } else if (txn->IsSharedLocked(rid) && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  iter->second.request_queue_.erase(target_pos);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  // 3.  If not only one, return.
  if (!only_one) {
    return true;
  }
  // 4.  If there is no other txns which are waiting for this rid, we delete this rid from map directly.
  if (iter->second.request_queue_.empty()) {
    lock_table_.erase(iter);
    return true;
  }
  // 5.  Grant the first write lock or grant read lock from head until encounter wirte lock.
  GrantLock(rid);
  return true;
}

}  // namespace bustub
