package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
)

// QualifyUncertainReplacement checks a fresh, exact-bound customer refund and
// original task lookup. Supplier cost remains unknown.
func QualifyUncertainReplacement(attempt TaskDispatchAttempt, verified TaskVerifiedReceipt, evidence []byte) bool {
	if attempt.Ordinal < 1 || attempt.ReceiptKind != "unknown" || attempt.ResolvedAt != 0 ||
		attempt.SupplierKey == "" || attempt.RequestHash == "" || attempt.AccountID == "" ||
		attempt.WatchUpstreamID == "" || attempt.WatchUpstreamID != verified.WatchUpstreamID ||
		verified.Kind != "unknown" || verified.NewSubmissionSafe || !verified.TaskLookupNotFound ||
		verified.FinalCustomerBillingReceiptID == "" || verified.ReceiptID == "" ||
		attempt.ReceiptID != verified.ReceiptID ||
		verified.ObservedAt < attempt.UnknownObservedAt || verified.ObservedAt < time.Now().Unix()-30 ||
		verified.ObservedAt > time.Now().Unix()+30 ||
		verified.AccountID != attempt.AccountID || verified.RequestHash != attempt.RequestHash ||
		verified.SupplierKey != attempt.SupplierKey || len(evidence) == 0 || len(evidence) > 64<<10 {
		return false
	}
	hash := sha256.Sum256(evidence)
	return verified.EvidenceHash == hex.EncodeToString(hash[:])
}

// AllocateUncertainReplacement assigns one new key under the dispatch lease.
// StartTaskSend later creates ordinal N+1; a crash before send reuses this key.
func AllocateUncertainReplacement(ctx context.Context, claim TaskDispatchClaim, original TaskDispatchAttempt,
	verified TaskVerifiedReceipt, evidence []byte) error {
	if !QualifyUncertainReplacement(original, verified, evidence) {
		return ErrTaskDispatchConflict
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", claim.TaskID).Take(&task).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", claim.TaskID, "reserved").Take(&finance).Error; err != nil {
			return err
		}
		hasOutcome, err := taskHasOutcomeTx(tx, claim.TaskID)
		if err != nil || hasOutcome {
			return ErrTaskDispatchConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).Take(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Version != claim.Version || dispatch.Owner != claim.Owner || dispatch.LeaseUntil <= time.Now().UnixMilli() ||
			dispatch.Phase != TaskDispatchUnknown || dispatch.Attempt != original.Ordinal ||
			dispatch.Attempt >= dispatch.AttemptBudget || dispatch.RetryPolicyVersion < 1 ||
			!dispatch.UncertainRetryEnabled || dispatch.UncertainReplacementUsed ||
			dispatch.RequestHash != original.RequestHash || dispatch.AccountID != original.AccountID ||
			dispatch.SupplierKey != original.SupplierKey ||
			time.Now().UnixMilli() < original.StartedAt+dispatch.UncertainRetryDelaySeconds*1000 {
			return ErrTaskDispatchConflict
		}
		var stored TaskDispatchAttempt
		if err := lockForUpdate(tx).Where("task_id = ? AND ordinal = ?", claim.TaskID, original.Ordinal).Take(&stored).Error; err != nil {
			return err
		}
		if stored.ReceiptKind != "unknown" || stored.ResolvedAt != 0 || stored.WatchState != "pending" ||
			stored.SupplierKey != original.SupplierKey || stored.RequestHash != original.RequestHash ||
			stored.AccountID != original.AccountID || stored.WatchUpstreamID != original.WatchUpstreamID {
			return ErrTaskDispatchConflict
		}
		key, err := newTaskSupplierKey()
		if err != nil {
			return err
		}
		basis, err := common.Marshal(map[string]any{
			"kind": "uncertain_replacement_allocation", "receipt_id": verified.ReceiptID,
			"evidence_hash": verified.EvidenceHash, "observed_at": verified.ObservedAt,
			"final_customer_billing_receipt_id": verified.FinalCustomerBillingReceiptID,
			"receipt_evidence":                  evidence,
		})
		if err != nil {
			return err
		}
		now := time.Now()
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ?", claim.TaskID, claim.Version).
			Updates(map[string]any{"version": claim.Version + 1, "phase": TaskDispatchReady, "stage_at": now.Unix(),
				"supplier_key": key, "uncertain_replacement_used": true,
				"next_attempt_at": max(dispatch.NextAttemptAt, now.UnixMilli()),
				"owner":           "", "lease_until": int64(0)})
		if result.Error != nil || result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		result = tx.Model(&TaskDispatchAttempt{}).Where("task_id = ? AND ordinal = ? AND resolved_at = 0 AND watch_state = ?", claim.TaskID, original.Ordinal, "pending").
			Updates(map[string]any{"task_lookup_not_found": true,
				"final_customer_billing_receipt_id": verified.FinalCustomerBillingReceiptID,
				"unknown_observed_at":               verified.ObservedAt, "evidence_hash": verified.EvidenceHash,
				"evidence": TaskBlob(evidence), "operator_audit": TaskBlob(basis),
				"reconcile_next_at": gorm.Expr("CASE WHEN reconcile_next_at > ? THEN reconcile_next_at ELSE ? END",
					now.Add(dispatch.ReconcileDelay()).UnixMilli(), now.Add(dispatch.ReconcileDelay()).UnixMilli())})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		return nil
	})
}

// CancelUncertainReplacement fences an allocated key before its first POST.
// The original remains eligible for read-only reconciliation.
func CancelUncertainReplacement(ctx context.Context, claim TaskDispatchClaim) error {
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", claim.TaskID).Take(&task).Error; err != nil {
			return err
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", claim.TaskID, "reserved").Take(&finance).Error; err != nil {
			return err
		}
		hasOutcome, err := taskHasOutcomeTx(tx, claim.TaskID)
		if err != nil {
			return err
		}
		if hasOutcome {
			return ErrTaskDispatchConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).Take(&dispatch).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure ||
			!dispatch.UncertainReplacementUsed || dispatch.UncertainReplacementCancelled || dispatch.Phase != TaskDispatchReady ||
			dispatch.Attempt != claim.Attempt || dispatch.Version != claim.Version ||
			dispatch.Owner != claim.Owner || dispatch.LeaseUntil <= time.Now().UnixMilli() {
			return ErrTaskDispatchConflict
		}
		var existing int64
		if err := tx.Model(&TaskDispatchAttempt{}).Where("task_id = ? AND ordinal = ?", claim.TaskID, dispatch.Attempt+1).
			Count(&existing).Error; err != nil {
			return err
		}
		if existing != 0 {
			return ErrTaskDispatchConflict
		}
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ?", claim.TaskID, claim.Version).
			Updates(map[string]any{"version": claim.Version + 1, "phase": TaskDispatchParked,
				"supplier_key": "", "uncertain_replacement_cancelled": true,
				"owner": "", "lease_until": int64(0), "next_attempt_at": int64(0),
				"stage_at": time.Now().Unix()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		return tx.Model(&Task{}).Where("id = ?", claim.TaskID).Updates(map[string]any{
			"status": TaskStatusUnknown, "progress": "Reconciliation", "updated_at": time.Now().Unix(),
		}).Error
	})
}

// ParkUncertainAttemptFailure leaves money reserved while another attempt can
// still produce a valid result. It cannot authorize another supplier key.
func ParkUncertainAttemptFailure(ctx context.Context, taskID int64, upstreamID string) error {
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", taskID).Take(&task).Error; err != nil {
			return err
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", taskID, "reserved").Take(&finance).Error; err != nil {
			return err
		}
		hasOutcome, err := taskHasOutcomeTx(tx, taskID)
		if err != nil {
			return err
		}
		if hasOutcome {
			return ErrTaskDispatchConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", taskID).Take(&dispatch).Error; err != nil {
			return err
		}
		if !dispatch.UncertainReplacementUsed || dispatch.WinnerOrdinal != 0 || dispatch.Phase != TaskDispatchAccepted ||
			dispatch.UpstreamID != upstreamID || task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ?", taskID, dispatch.Version).
			Updates(map[string]any{"version": dispatch.Version + 1, "phase": TaskDispatchParked,
				"owner": "", "lease_until": int64(0), "next_attempt_at": int64(0), "stage_at": time.Now().Unix()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		return tx.Model(&Task{}).Where("id = ?", taskID).Updates(map[string]any{
			"status": TaskStatusUnknown, "progress": "Reconciliation", "updated_at": time.Now().Unix(),
		}).Error
	})
}

// SelectUncertainCandidate fixes the first confirmed result before any archive.
// Only this identity can subsequently use the shared customer artifact path.
func SelectUncertainCandidate(ctx context.Context, taskID int64, ordinal int, upstreamID string, evidence []byte) error {
	if taskID < 1 || ordinal < 1 || upstreamID == "" || len(upstreamID) > 191 ||
		len(evidence) == 0 || len(evidence) > 64<<10 {
		return ErrTaskDispatchConflict
	}
	evidenceHash := sha256.Sum256(evidence)
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", taskID).Take(&task).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", taskID, "reserved").Take(&finance).Error; err != nil {
			return err
		}
		hasOutcome, err := taskHasOutcomeTx(tx, taskID)
		if err != nil || hasOutcome {
			return ErrTaskDispatchConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", taskID).Take(&dispatch).Error; err != nil {
			return err
		}
		if !dispatch.UncertainReplacementUsed || dispatch.WinnerOrdinal != 0 || ordinal > dispatch.Attempt ||
			(dispatch.Phase != TaskDispatchAccepted && dispatch.Phase != TaskDispatchUnknown && dispatch.Phase != TaskDispatchParked) {
			return ErrTaskDispatchConflict
		}
		var winningAttempt TaskDispatchAttempt
		if err := tx.Where("task_id = ? AND ordinal = ?", taskID, ordinal).Take(&winningAttempt).Error; err != nil {
			return err
		}
		if winningAttempt.WatchState != "pending" ||
			(winningAttempt.WatchUpstreamID != upstreamID && (ordinal != dispatch.Attempt || dispatch.UpstreamID != upstreamID)) {
			return ErrTaskDispatchConflict
		}
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ? AND winner_ordinal = 0", taskID, dispatch.Version).
			Updates(map[string]any{"version": dispatch.Version + 1, "winner_ordinal": ordinal,
				"phase": TaskDispatchAccepted, "upstream_id": upstreamID, "stage_at": time.Now().Unix(),
				"owner": "", "lease_until": int64(0),
				"next_attempt_at": max(dispatch.NextAttemptAt, time.Now().UnixMilli())})
		if result.Error != nil || result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		task.PrivateData.UpstreamTaskID = upstreamID
		if err := tx.Model(&Task{}).Where("id = ?", taskID).Update("private_data", task.PrivateData).Error; err != nil {
			return err
		}
		result = tx.Model(&TaskDispatchAttempt{}).Where("task_id = ? AND ordinal = ?", taskID, ordinal).
			Updates(map[string]any{"watch_result_status": TaskStatusSuccess, "watch_evidence_hash": hex.EncodeToString(evidenceHash[:]),
				"watch_observed_at": time.Now().Unix()})
		if result.Error != nil || result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		return nil
	})
}

// PersistUncertainFinalFailure closes only two independently proven final
// failures. One unknown or active attempt cannot refund the customer task.
func PersistUncertainFinalFailure(ctx context.Context, taskID int64, retainResult bool) (TaskOutcome, int64, error) {
	var outcome TaskOutcome
	var version int64
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", taskID).Take(&task).Error; err != nil {
			return err
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", taskID, "reserved").Take(&finance).Error; err != nil {
			return err
		}
		hasOutcome, err := taskHasOutcomeTx(tx, taskID)
		if err != nil {
			return err
		}
		if hasOutcome {
			return ErrTaskDispatchConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", taskID).Take(&dispatch).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure || !dispatch.UncertainReplacementUsed ||
			dispatch.WinnerOrdinal != 0 || finance.Version < 1 {
			return ErrTaskDispatchConflict
		}
		var attempts []TaskDispatchAttempt
		if dispatch.UncertainReplacementCancelled {
			if dispatch.Phase != TaskDispatchParked || dispatch.SupplierKey != "" || dispatch.Attempt < 1 {
				return ErrTaskDispatchConflict
			}
			var sent int64
			if err := tx.Model(&TaskDispatchAttempt{}).Where("task_id = ? AND ordinal = ?", taskID, dispatch.Attempt+1).
				Count(&sent).Error; err != nil {
				return err
			}
			if sent != 0 {
				return ErrTaskDispatchConflict
			}
			if err := lockForUpdate(tx).Where("task_id = ? AND ordinal = ?", taskID, dispatch.Attempt).
				Find(&attempts).Error; err != nil {
				return err
			}
		} else {
			if dispatch.Attempt < 2 {
				return ErrTaskDispatchConflict
			}
			if err := lockForUpdate(tx).Where("task_id = ? AND ordinal IN ?", taskID,
				[]int{dispatch.Attempt - 1, dispatch.Attempt}).Order("ordinal asc").Find(&attempts).Error; err != nil {
				return err
			}
		}
		if len(attempts) != 2 && (!dispatch.UncertainReplacementCancelled || len(attempts) != 1) {
			return ErrTaskDispatchConflict
		}
		for _, attempt := range attempts {
			if attempt.WatchState != "observed" || attempt.WatchResultStatus != TaskStatusFailure ||
				attempt.WatchFinalBillingReceiptID == "" || attempt.WatchPublicErrorCode == "" ||
				strings.Trim(attempt.WatchPublicErrorCode, "abcdefghijklmnopqrstuvwxyz0123456789_") != "" {
				return ErrTaskDispatchConflict
			}
		}
		outcome = TaskOutcome{Status: TaskStatusFailure, ActualQuota: 0, ErrorCode: attempts[len(attempts)-1].WatchPublicErrorCode,
			FailReason: "Task failed", RetainResult: retainResult}
		if err := validateTaskOutcome(outcome); err != nil {
			return err
		}
		result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ? AND winner_ordinal = 0", taskID, dispatch.Version).
			Updates(map[string]any{"version": dispatch.Version + 1, "phase": TaskDispatchRejected,
				"owner": "", "lease_until": int64(0), "next_attempt_at": int64(0), "stage_at": time.Now().Unix()})
		if result.Error != nil || result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		dispatch.Phase = TaskDispatchRejected
		hash := canonicalTaskOutcomeHash(taskID, finance.Version, dispatch, outcome)
		if err := tx.Create(&PendingTaskOutcome{TaskID: taskID, FinanceVersion: finance.Version, Hash: hash,
			Status: outcome.Status, ActualQuota: 0, RetainResult: retainResult, ErrorCode: outcome.ErrorCode,
			FailReason: outcome.FailReason, State: "pending", CreatedAt: time.Now().Unix()}).Error; err != nil {
			return err
		}
		version = finance.Version
		return nil
	})
	return outcome, version, err
}

func ListDueUncertainOriginals(ctx context.Context, afterTaskID int64, limit int) ([]TaskDispatchAttempt, error) {
	if limit < 1 || limit > 100 {
		limit = 8
	}
	var rows []TaskDispatchAttempt
	err := DB.WithContext(ctx).Model(&TaskDispatchAttempt{}).
		Joins("JOIN task_dispatches ON task_dispatches.task_id = task_dispatch_attempts.task_id").
		Where("task_dispatch_attempts.task_id > ? AND task_dispatches.uncertain_replacement_used = ? AND task_dispatch_attempts.watch_state = ? AND task_dispatch_attempts.reconcile_next_at <= ?", afterTaskID,
			true, "pending", time.Now().UnixMilli()).Order("task_dispatch_attempts.task_id asc, task_dispatch_attempts.ordinal asc").Limit(limit).Find(&rows).Error
	return rows, err
}

func ListUnsettledUncertainFailures(ctx context.Context, afterTaskID int64, limit int) ([]int64, error) {
	if limit < 1 || limit > 100 {
		limit = 8
	}
	var ids []int64
	err := DB.WithContext(ctx).Model(&TaskDispatch{}).
		Where("task_dispatches.task_id > ? AND task_dispatches.uncertain_replacement_used = ? AND task_dispatches.winner_ordinal = 0",
			afterTaskID, true).
		Where("EXISTS (SELECT 1 FROM task_finances f WHERE f.task_id = task_dispatches.task_id AND f.state = ?)", "reserved").
		Where("((task_dispatches.uncertain_replacement_cancelled = ? AND task_dispatches.phase = ? AND task_dispatches.supplier_key = '' AND NOT EXISTS (SELECT 1 FROM task_dispatch_attempts sent WHERE sent.task_id = task_dispatches.task_id AND sent.ordinal = task_dispatches.attempt + 1) AND (SELECT count(*) FROM task_dispatch_attempts a WHERE a.task_id = task_dispatches.task_id AND a.ordinal = task_dispatches.attempt AND a.watch_state = ? AND a.watch_result_status = ? AND a.watch_final_billing_receipt_id <> '' AND a.watch_public_error_code <> '') = 1) OR (task_dispatches.uncertain_replacement_cancelled = ? AND (SELECT count(*) FROM task_dispatch_attempts a WHERE a.task_id = task_dispatches.task_id AND a.ordinal IN (task_dispatches.attempt - 1, task_dispatches.attempt) AND a.watch_state = ? AND a.watch_result_status = ? AND a.watch_final_billing_receipt_id <> '' AND a.watch_public_error_code <> '') = 2))",
			true, TaskDispatchParked, "observed", TaskStatusFailure, false, "observed", TaskStatusFailure).
		Order("task_dispatches.task_id asc").Limit(limit).Pluck("task_dispatches.task_id", &ids).Error
	return ids, err
}

func DeferUncertainOriginal(ctx context.Context, attempt TaskDispatchAttempt, delay time.Duration) error {
	if delay < time.Second {
		delay = time.Second
	}
	for i := 0; i < min(attempt.WatchPollCount, 6); i++ {
		delay *= 2
	}
	if delay > time.Hour {
		delay = time.Hour
	}
	result := DB.WithContext(ctx).Model(&TaskDispatchAttempt{}).
		Where("task_id = ? AND ordinal = ? AND watch_state = ? AND watch_poll_count = ?", attempt.TaskID, attempt.Ordinal, "pending", attempt.WatchPollCount).
		Updates(map[string]any{"reconcile_next_at": gorm.Expr("CASE WHEN reconcile_next_at > ? THEN reconcile_next_at ELSE ? END",
			time.Now().Add(delay).UnixMilli(), time.Now().Add(delay).UnixMilli()), "watch_poll_count": attempt.WatchPollCount + 1})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrTaskDispatchConflict
	}
	return nil
}

func PersistTaskReceiptRetryFloor(ctx context.Context, claim TaskDispatchClaim, notBeforeAt int64) error {
	if notBeforeAt <= 0 {
		return nil
	}
	if notBeforeAt > time.Now().Add(time.Hour).UnixMilli()+1000 {
		return ErrTaskDispatchConflict
	}
	result := DB.WithContext(ctx).Model(&TaskDispatch{}).
		Where("task_id = ? AND version = ? AND owner = ? AND lease_until > ?", claim.TaskID, claim.Version,
			claim.Owner, time.Now().UnixMilli()).
		Update("next_attempt_at", gorm.Expr("CASE WHEN next_attempt_at > ? THEN next_attempt_at ELSE ? END", notBeforeAt, notBeforeAt))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrTaskDispatchConflict
	}
	return nil
}

func PersistAttemptReceiptRetryFloor(ctx context.Context, attempt TaskDispatchAttempt, notBeforeAt int64) error {
	if notBeforeAt <= 0 {
		return nil
	}
	if notBeforeAt > time.Now().Add(time.Hour).UnixMilli()+1000 {
		return ErrTaskDispatchConflict
	}
	result := DB.WithContext(ctx).Model(&TaskDispatchAttempt{}).
		Where("task_id = ? AND ordinal = ? AND watch_state = ?", attempt.TaskID, attempt.Ordinal, "pending").
		Update("reconcile_next_at", gorm.Expr("CASE WHEN reconcile_next_at > ? THEN reconcile_next_at ELSE ? END", notBeforeAt, notBeforeAt))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrTaskDispatchConflict
	}
	return nil
}

func AuditUncertainOriginal(ctx context.Context, attempt TaskDispatchAttempt, state TaskStatus, upstreamID string,
	publicCode, billingReceiptID string, evidence []byte) error {
	if len(evidence) == 0 || len(evidence) > 64<<10 || len(upstreamID) > 191 ||
		(state != TaskStatusSuccess && state != TaskStatusFailure) ||
		(state == TaskStatusFailure && (publicCode == "" || billingReceiptID == "" || len(publicCode) > 64 ||
			len(billingReceiptID) > 191 || strings.Trim(publicCode, "abcdefghijklmnopqrstuvwxyz0123456789_") != "")) {
		return ErrTaskDispatchConflict
	}
	hash := sha256.Sum256(evidence)
	result := DB.WithContext(ctx).Model(&TaskDispatchAttempt{}).
		Where("task_id = ? AND ordinal = ? AND watch_state = ?", attempt.TaskID, attempt.Ordinal, "pending").
		Updates(map[string]any{"watch_state": "observed", "watch_result_status": state,
			"watch_upstream_id": upstreamID, "watch_evidence_hash": hex.EncodeToString(hash[:]),
			"watch_public_error_code": publicCode, "watch_final_billing_receipt_id": billingReceiptID,
			"watch_observed_at": time.Now().Unix(), "reconcile_next_at": int64(0)})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrTaskDispatchConflict
	}
	return nil
}

// AuditUncertainRefusal records a final by-key refusal even when no task ID
// exists to query. It never changes the original acceptance classification.
func AuditUncertainRefusal(ctx context.Context, attempt TaskDispatchAttempt, verified TaskVerifiedReceipt, evidence []byte) error {
	if verified.Kind != "rejected" && verified.Kind != "not_accepted" ||
		verified.AccountID != attempt.AccountID || verified.RequestHash != attempt.RequestHash ||
		verified.SupplierKey != attempt.SupplierKey || verified.ReceiptID == "" || verified.ObservedAt <= 0 ||
		verified.FinalCustomerBillingReceiptID == "" ||
		verified.PublicErrorCode == "" || !verified.SupplierBillingFinal || verified.SupplierNetQuota != 0 ||
		len(evidence) == 0 || len(evidence) > 64<<10 ||
		strings.Trim(verified.PublicErrorCode, "abcdefghijklmnopqrstuvwxyz0123456789_") != "" {
		return ErrTaskDispatchConflict
	}
	hash := sha256.Sum256(evidence)
	if hex.EncodeToString(hash[:]) != verified.EvidenceHash {
		return ErrTaskDispatchConflict
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", attempt.TaskID).Take(&task).Error; err != nil {
			return err
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ?", attempt.TaskID).Take(&finance).Error; err != nil {
			return err
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", attempt.TaskID).Take(&dispatch).Error; err != nil {
			return err
		}
		if !dispatch.UncertainReplacementUsed || attempt.Ordinal > dispatch.Attempt {
			return ErrTaskDispatchConflict
		}
		var stored TaskDispatchAttempt
		if err := lockForUpdate(tx).Where("task_id = ? AND ordinal = ?", attempt.TaskID, attempt.Ordinal).Take(&stored).Error; err != nil {
			return err
		}
		if stored.WatchState != "pending" || stored.AccountID != verified.AccountID ||
			stored.RequestHash != verified.RequestHash || stored.SupplierKey != verified.SupplierKey ||
			(stored.ReceiptID != "" && stored.ReceiptID != verified.ReceiptID) ||
			verified.ObservedAt < stored.UnknownObservedAt ||
			(stored.ReceiptKind != "" && stored.ReceiptKind != "unknown") {
			return ErrTaskDispatchConflict
		}
		result := tx.Model(&TaskDispatchAttempt{}).Where("task_id = ? AND ordinal = ? AND watch_state = ?",
			attempt.TaskID, attempt.Ordinal, "pending").Updates(map[string]any{
			"watch_state": "observed", "watch_result_status": TaskStatusFailure,
			"watch_public_error_code":        verified.PublicErrorCode,
			"watch_final_billing_receipt_id": verified.FinalCustomerBillingReceiptID,
			"watch_evidence_hash":            verified.EvidenceHash, "watch_observed_at": time.Now().Unix(),
			"receipt_kind": verified.Kind, "receipt_id": verified.ReceiptID,
			"evidence_hash": verified.EvidenceHash, "evidence": TaskBlob(evidence),
			"unknown_observed_at": verified.ObservedAt, "reconcile_next_at": int64(0),
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		return nil
	})
}

func RefreshUncertainWatchIdentity(ctx context.Context, attempt TaskDispatchAttempt, verified TaskVerifiedReceipt, evidence []byte) error {
	upstreamID := verified.WatchUpstreamID
	if verified.Kind == "accepted" {
		upstreamID = verified.UpstreamID
	}
	if (verified.Kind != "unknown" && verified.Kind != "accepted") || upstreamID == "" ||
		len(upstreamID) > 191 || verified.ReceiptID == "" || verified.ObservedAt <= 0 ||
		verified.AccountID != attempt.AccountID || verified.RequestHash != attempt.RequestHash ||
		verified.SupplierKey != attempt.SupplierKey || len(evidence) == 0 || len(evidence) > 64<<10 {
		return ErrTaskDispatchConflict
	}
	hash := sha256.Sum256(evidence)
	if hex.EncodeToString(hash[:]) != verified.EvidenceHash {
		return ErrTaskDispatchConflict
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", attempt.TaskID).Take(&task).Error; err != nil {
			return err
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ?", attempt.TaskID).Take(&finance).Error; err != nil {
			return err
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", attempt.TaskID).Take(&dispatch).Error; err != nil {
			return err
		}
		if !dispatch.UncertainReplacementUsed || attempt.Ordinal > dispatch.Attempt {
			return ErrTaskDispatchConflict
		}
		var stored TaskDispatchAttempt
		if err := lockForUpdate(tx).Where("task_id = ? AND ordinal = ?", attempt.TaskID, attempt.Ordinal).Take(&stored).Error; err != nil {
			return err
		}
		if stored.WatchState != "pending" || stored.AccountID != verified.AccountID ||
			stored.RequestHash != verified.RequestHash || stored.SupplierKey != verified.SupplierKey ||
			(stored.ReceiptID != "" && stored.ReceiptID != verified.ReceiptID) ||
			(stored.WatchUpstreamID != "" && stored.WatchUpstreamID != upstreamID) ||
			(stored.ReceiptKind == "accepted" && verified.Kind != "accepted") ||
			(stored.ReceiptKind != "" && stored.ReceiptKind != "unknown" && stored.ReceiptKind != "accepted") ||
			verified.ObservedAt < stored.UnknownObservedAt {
			return ErrTaskDispatchConflict
		}
		result := tx.Model(&TaskDispatchAttempt{}).Where("task_id = ? AND ordinal = ? AND watch_state = ?",
			attempt.TaskID, attempt.Ordinal, "pending").Updates(map[string]any{
			"watch_upstream_id": upstreamID, "receipt_kind": verified.Kind,
			"receipt_id": verified.ReceiptID, "evidence_hash": verified.EvidenceHash,
			"evidence": TaskBlob(evidence), "unknown_observed_at": verified.ObservedAt,
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		return nil
	})
}
