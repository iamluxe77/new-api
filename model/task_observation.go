package model

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
)

// TaskHTTPObservation is private recovery input. A terminal response is kept
// until an outcome is persisted; neither this row nor its headers are public.
type TaskHTTPObservation struct {
	TaskID           int64 `gorm:"primaryKey;autoIncrement:false"`
	Attempt          int
	Kind             string `gorm:"type:varchar(16);not null"`
	UpstreamID       string `gorm:"type:varchar(191)"`
	StatusCode       int
	Headers          TaskBlob `json:"-"`
	HeadersTruncated bool
	Body             TaskBlob `json:"-"`
	BodySHA256       string   `json:"-" gorm:"type:char(64)"`
	BodyLength       int64
	BodyTruncated    bool
	BodyReadError    bool
	ObservedAt       int64
}

const MaxTaskHTTPObservationBytes = MaxPendingTaskOutcomeBytes
const MaxTaskSubmitAuditBodyBytes = 32 << 10

func GetTaskHTTPObservation(ctx context.Context, taskID int64) (*TaskHTTPObservation, error) {
	var row TaskHTTPObservation
	err := DB.WithContext(ctx).Where("task_id = ?", taskID).Take(&row).Error
	return &row, err
}

// PersistTaskHTTPObservation fixes the first unprocessed supplier response.
// Replacing it would allow a later poll to erase a ready result after a crash.
func PersistTaskHTTPObservation(ctx context.Context, claim TaskDispatchClaim, observation TaskHTTPObservation) error {
	if claim.TaskID <= 0 || claim.Owner == "" || observation.TaskID != claim.TaskID ||
		(observation.Kind != "submit" && observation.Kind != "adopt" && observation.Kind != "query") ||
		observation.StatusCode < 100 || observation.StatusCode > 599 ||
		len(observation.Body) > MaxTaskHTTPObservationBytes || len(observation.Headers) > 16<<10 ||
		len(observation.UpstreamID) > 191 {
		return ErrTaskDispatchConflict
	}
	var header http.Header
	if err := common.Unmarshal(observation.Headers, &header); err != nil {
		return ErrTaskDispatchConflict
	}
	observation.Attempt = claim.Attempt
	if observation.ObservedAt == 0 {
		observation.ObservedAt = time.Now().Unix()
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", claim.TaskID).First(&task).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskDispatchConflict
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ? AND state = ?", claim.TaskID, "reserved").First(&finance).Error; err != nil {
			return err
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Owner != claim.Owner || dispatch.Version != claim.Version ||
			dispatch.LeaseUntil <= time.Now().UnixMilli() || dispatch.Attempt != claim.Attempt ||
			(observation.Kind == "submit" && dispatch.Phase != TaskDispatchSendStarted) ||
			(observation.Kind == "adopt" && dispatch.Phase != TaskDispatchAdopting) ||
			(observation.Kind == "query" && dispatch.Phase != TaskDispatchAccepted) {
			return ErrTaskDispatchConflict
		}
		if observation.Kind == "submit" && dispatch.AtomicReplaySeconds > 0 {
			var attempt TaskDispatchAttempt
			if err := lockForUpdate(tx).Where("task_id = ? AND ordinal = ?", claim.TaskID, claim.Attempt).First(&attempt).Error; err != nil {
				return err
			}
			if attempt.SupplierKey != dispatch.SupplierKey || attempt.AccountID != dispatch.AccountID ||
				attempt.RequestHash != dispatch.RequestHash {
				return ErrTaskDispatchConflict
			}
			if len(observation.Body) > MaxTaskSubmitAuditBodyBytes || observation.BodyLength < int64(len(observation.Body)) ||
				observation.BodyTruncated != (observation.BodyLength > int64(len(observation.Body)) || observation.BodyReadError) ||
				len(observation.BodySHA256) != 64 {
				return ErrTaskDispatchConflict
			}
			if attempt.ReplayCount == 0 {
				if attempt.SubmitObservedAt != 0 {
					return ErrTaskDispatchConflict
				}
				result := tx.Model(&TaskDispatchAttempt{}).Where("task_id = ? AND ordinal = ? AND submit_observed_at = 0",
					claim.TaskID, claim.Attempt).Updates(map[string]any{"submit_status_code": observation.StatusCode,
					"submit_headers": TaskBlob(observation.Headers), "submit_headers_truncated": observation.HeadersTruncated,
					"submit_body":        TaskBlob(observation.Body),
					"submit_body_sha256": observation.BodySHA256, "submit_body_length": observation.BodyLength,
					"submit_body_truncated": observation.BodyTruncated, "submit_body_read_error": observation.BodyReadError,
					"submit_observed_at": observation.ObservedAt})
				if result.Error != nil {
					return result.Error
				}
				if result.RowsAffected != 1 {
					return ErrTaskDispatchConflict
				}
			} else {
				if err := tx.Create(&TaskDispatchReplayObservation{TaskID: claim.TaskID, Ordinal: claim.Attempt,
					ReplayNumber: attempt.ReplayCount, StatusCode: observation.StatusCode,
					Headers: TaskBlob(observation.Headers), HeadersTruncated: observation.HeadersTruncated,
					Body:       TaskBlob(observation.Body),
					BodySHA256: observation.BodySHA256, BodyLength: observation.BodyLength,
					BodyTruncated: observation.BodyTruncated, BodyReadError: observation.BodyReadError,
					ObservedAt: observation.ObservedAt}).Error; err != nil {
					return err
				}
			}
		}
		var saved TaskHTTPObservation
		err := tx.Where("task_id = ?", claim.TaskID).Take(&saved).Error
		if err == nil {
			return ErrTaskDispatchConflict
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err := tx.Create(&observation).Error; err != nil {
			return err
		}
		if observation.Kind == "submit" && dispatch.AtomicReplaySeconds > 0 {
			deadline := taskSubmitRetryAfterDeadline(header, observation.ObservedAt)
			if deadline > dispatch.NextAttemptAt {
				result := tx.Model(&TaskDispatch{}).Where("task_id = ? AND version = ? AND owner = ?",
					claim.TaskID, claim.Version, claim.Owner).Update("next_attempt_at", deadline)
				if result.Error != nil {
					return result.Error
				}
				if result.RowsAffected != 1 {
					return ErrTaskDispatchConflict
				}
			}
		}
		return nil
	})
}

func taskSubmitRetryAfterDeadline(headers http.Header, observedAt int64) int64 {
	raw := headers.Get("Retry-After")
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil && seconds > 0 {
		return (observedAt + min(seconds, 3600)) * 1000
	}
	if deadline, err := http.ParseTime(raw); err == nil {
		return min(deadline.Unix(), observedAt+3600) * 1000
	}
	return 0
}

func ClearTaskHTTPObservation(ctx context.Context, claim TaskDispatchClaim, expected TaskHTTPObservation) error {
	if expected.TaskID != claim.TaskID || expected.Attempt != claim.Attempt || expected.ObservedAt <= 0 {
		return ErrTaskDispatchConflict
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", claim.TaskID).First(&task).Error; err != nil {
			return err
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).First(&finance).Error; err != nil {
			return err
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).First(&dispatch).Error; err != nil {
			return err
		}
		active := dispatch.Version == claim.Version && dispatch.Owner == claim.Owner &&
			dispatch.LeaseUntil > time.Now().UnixMilli() && dispatch.Phase == claim.Phase
		// AcceptTaskDispatch releases the submit claim before terminal outcome
		// persistence. The pending outcome fences any new dispatch claim.
		terminal := dispatch.Version == claim.Version+1 && dispatch.Phase == TaskDispatchAccepted &&
			dispatch.Owner == "" && (claim.Phase == TaskDispatchSendStarted || claim.Phase == TaskDispatchAdopting) &&
			(expected.Kind == "submit" || expected.Kind == "adopt")
		if terminal {
			var pending PendingTaskOutcome
			terminal = tx.Where("task_id = ? AND state = ?", claim.TaskID, "pending").Take(&pending).Error == nil
		}
		if !active && !terminal {
			return ErrTaskDispatchConflict
		}
		if claim.Attempt > 0 {
			var attempt TaskDispatchAttempt
			if err := lockForUpdate(tx).Where("task_id = ? AND ordinal = ?", claim.TaskID, claim.Attempt).First(&attempt).Error; err != nil {
				return err
			}
		}
		var saved TaskHTTPObservation
		if err := lockForUpdate(tx).Where("task_id = ?", claim.TaskID).First(&saved).Error; err != nil {
			return err
		}
		if saved.Attempt != expected.Attempt || saved.Kind != expected.Kind || saved.StatusCode != expected.StatusCode ||
			saved.ObservedAt != expected.ObservedAt || saved.UpstreamID != expected.UpstreamID ||
			saved.BodySHA256 != expected.BodySHA256 || saved.BodyLength != expected.BodyLength ||
			saved.BodyTruncated != expected.BodyTruncated || saved.BodyReadError != expected.BodyReadError ||
			saved.HeadersTruncated != expected.HeadersTruncated ||
			!bytes.Equal(saved.Headers, expected.Headers) || !bytes.Equal(saved.Body, expected.Body) {
			return ErrTaskDispatchConflict
		}
		result := tx.Where("task_id = ? AND attempt = ? AND kind = ? AND observed_at = ?", claim.TaskID,
			claim.Attempt, expected.Kind, expected.ObservedAt).Delete(&TaskHTTPObservation{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskDispatchConflict
		}
		return nil
	})
}
