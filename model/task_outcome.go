package model

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
)

const MaxPendingTaskOutcomeBytes = 128 << 20

// TaskPublicFailureMessage maps trusted semantic codes to fixed public text.
func TaskPublicFailureMessage(code string) string {
	switch code {
	case "invalid_input":
		return "Invalid task input"
	case "unsupported_input":
		return "Unsupported task input"
	case "content_policy_violation":
		return "Task content was rejected"
	case "invalid_reference_geometry":
		return "Invalid reference image geometry"
	case "reference_unavailable":
		return "The reference files could not be loaded. Please contact support."
	case "reference_not_ready":
		return "The reference media is awaiting certification. Please wait until certification is complete."
	default:
		return "Task failed"
	}
}

// PendingTaskOutcome is the private primary-DB recovery input, not a second
// ledger. It is written before terminal money or public Task state changes.
type PendingTaskOutcome struct {
	TaskID         int64 `gorm:"primaryKey;autoIncrement:false"`
	FinanceVersion int64
	Hash           string     `gorm:"type:char(64);not null"`
	UpstreamID     string     `gorm:"type:varchar(191)"`
	Status         TaskStatus `gorm:"type:varchar(20);not null"`
	ActualQuota    int
	Data           TaskBlob `json:"-"`
	ResultURL      string   `json:"-" gorm:"type:text"`
	PluginState    TaskBlob `json:"-"`
	UsageFacts     TaskBlob `json:"-"`
	RetainResult   bool
	ErrorCode      string `gorm:"type:varchar(64)"`
	FailReason     string `gorm:"type:text"`
	State          string `gorm:"type:varchar(20);not null;index:idx_pending_task_outcome_state,priority:1"`
	CreatedAt      int64  `gorm:"type:bigint;index:idx_pending_task_outcome_state,priority:2"`
	NextAttemptAt  int64  `gorm:"type:bigint;index:idx_pending_task_outcome_due"`
	FailureCount   int
}

func validateTaskOutcome(outcome TaskOutcome) error {
	if outcome.ActualQuota < 0 || outcome.ActualQuota > common.MaxQuota ||
		(outcome.Status != TaskStatusSuccess && outcome.Status != TaskStatusFailure) ||
		(outcome.Status == TaskStatusFailure && outcome.ActualQuota != 0) ||
		len(outcome.TaskID) > 191 || len(outcome.Data) > MaxPendingTaskOutcomeBytes ||
		len(outcome.FailReason) > 4096 || len(outcome.ErrorCode) > 64 ||
		len(outcome.ResultURL) > 4096 || len(outcome.PluginState) > 1<<20 || len(outcome.UsageFacts) > 1<<20 ||
		!utf8.ValidString(outcome.TaskID) || !utf8.ValidString(outcome.FailReason) ||
		!utf8.ValidString(outcome.ResultURL) ||
		strings.ContainsFunc(outcome.TaskID, unicode.IsControl) ||
		strings.ContainsFunc(outcome.FailReason, unicode.IsControl) {
		return ErrTaskFinanceInvalid
	}
	// Runtime supplies a bounded, sanitized public reason and a typed code.
	// This layer never copies provider error prose into FailReason itself.
	if outcome.Status == TaskStatusSuccess && (outcome.FailReason != "" || outcome.ErrorCode != "") {
		return ErrTaskFinanceInvalid
	}
	if outcome.Status == TaskStatusFailure {
		if outcome.ErrorCode == "" || outcome.FailReason == "" || strings.Trim(outcome.ErrorCode, "abcdefghijklmnopqrstuvwxyz0123456789_") != "" {
			return ErrTaskFinanceInvalid
		}
	}
	if len(outcome.Data) > 0 {
		var decoded any
		if err := common.Unmarshal(outcome.Data, &decoded); err != nil {
			return ErrTaskFinanceInvalid
		}
	}
	if len(outcome.PluginState) > 0 {
		var decoded any
		if err := common.Unmarshal(outcome.PluginState, &decoded); err != nil {
			return ErrTaskFinanceInvalid
		}
	}
	if len(outcome.UsageFacts) > 0 {
		var decoded map[string]any
		if err := common.Unmarshal(outcome.UsageFacts, &decoded); err != nil || decoded == nil {
			return ErrTaskFinanceInvalid
		}
	}
	return nil
}

// Call only after locking the Task and its Finance row. PersistTaskOutcome
// takes those same locks before inserting, so this absence check is a barrier.
func taskHasOutcomeTx(tx *gorm.DB, taskID int64) (bool, error) {
	var outcome PendingTaskOutcome
	err := tx.Select("task_id").Where("task_id = ?", taskID).Take(&outcome).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return err == nil, err
}

// PersistTaskOutcome fixes the exact provider result before settlement. A
// conflict cannot replace a successful generation with another outcome.
func PersistTaskOutcome(ctx context.Context, taskID, version int64, outcome TaskOutcome) error {
	if taskID <= 0 || version <= 0 {
		return ErrTaskFinanceInvalid
	}
	if err := validateTaskOutcome(outcome); err != nil {
		return err
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", taskID).First(&task).Error; err != nil {
			return err
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ?", taskID).First(&finance).Error; err != nil {
			return err
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", taskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Phase == TaskDispatchSendStarted || dispatch.Phase == TaskDispatchUnknown || dispatch.Phase == TaskDispatchParked ||
			(outcome.Status == TaskStatusSuccess && (dispatch.Phase != TaskDispatchAccepted || outcome.TaskID == "" || outcome.TaskID != dispatch.UpstreamID)) ||
			(dispatch.Phase == TaskDispatchAccepted && outcome.TaskID != dispatch.UpstreamID) {
			return ErrTaskFinanceConflict
		}
		hash := canonicalTaskOutcomeHash(taskID, version, dispatch, outcome)
		if outcome.OutcomeHash != "" && outcome.OutcomeHash != hash {
			return ErrTaskFinanceInvalid
		}
		var saved PendingTaskOutcome
		err := lockForUpdate(tx).Where("task_id = ?", taskID).First(&saved).Error
		if err == nil {
			if saved.Hash == hash && saved.FinanceVersion == version && saved.State == "pending" {
				return nil
			}
			if saved.Hash == hash && saved.State == "settled" && finance.OutcomeHash == hash {
				return nil
			}
			return ErrTaskFinanceConflict
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if finance.State != "reserved" || finance.Version != version || task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskFinanceConflict
		}
		return tx.Create(&PendingTaskOutcome{TaskID: taskID, FinanceVersion: version, Hash: hash,
			UpstreamID: outcome.TaskID, Status: outcome.Status, ActualQuota: outcome.ActualQuota,
			Data: TaskBlob(outcome.Data), ResultURL: outcome.ResultURL, PluginState: TaskBlob(outcome.PluginState),
			UsageFacts: TaskBlob(outcome.UsageFacts), RetainResult: outcome.RetainResult,
			ErrorCode: outcome.ErrorCode, FailReason: outcome.FailReason,
			State: "pending", CreatedAt: time.Now().Unix()}).Error
	})
}

func ListPendingTaskOutcomes(ctx context.Context, limit int) ([]PendingTaskOutcome, error) {
	return ListPendingTaskOutcomesAfter(ctx, 0, limit)
}

// The cursor lets workers pass a row that requires operator action without
// repeatedly hiding later payable outcomes behind the oldest page.
func ListPendingTaskOutcomesAfter(ctx context.Context, afterTaskID int64, limit int) ([]PendingTaskOutcome, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var outcomes []PendingTaskOutcome
	err := DB.WithContext(ctx).Where("state = ? AND task_id > ? AND next_attempt_at <= ?", "pending", afterTaskID, time.Now().Unix()).
		Order("task_id asc").Limit(limit).Find(&outcomes).Error
	return outcomes, err
}

func RetryPendingTaskOutcome(ctx context.Context, taskID int64) error {
	var pending PendingTaskOutcome
	if err := DB.WithContext(ctx).Where("task_id = ? AND state = ?", taskID, "pending").First(&pending).Error; err != nil {
		return err
	}
	return CommitTaskOutcome(ctx, taskID, pending.FinanceVersion, TaskOutcome{Status: pending.Status,
		ActualQuota: pending.ActualQuota, OutcomeHash: pending.Hash, TaskID: pending.UpstreamID,
		Data: []byte(pending.Data), ResultURL: pending.ResultURL, PluginState: []byte(pending.PluginState),
		UsageFacts: []byte(pending.UsageFacts), RetainResult: pending.RetainResult,
		ErrorCode: pending.ErrorCode, FailReason: pending.FailReason})
}

func deferPendingTaskOutcome(ctx context.Context, taskID int64) {
	_ = DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var pending PendingTaskOutcome
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", taskID, "pending").First(&pending).Error; err != nil {
			return err
		}
		attempt := min(pending.FailureCount+1, 16)
		delay := min(int64(60)<<min(attempt-1, 6), int64(3600))
		return tx.Model(&PendingTaskOutcome{}).Where("task_id = ? AND state = ?", taskID, "pending").
			Updates(map[string]any{"failure_count": attempt, "next_attempt_at": time.Now().Unix() + delay}).Error
	})
}
