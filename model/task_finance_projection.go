package model

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/pkg/billingexpr"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// The primary event owns the exact payload sent to each projection. Neither
// projection is allowed to recalculate an old event from current settings.
func createTaskFinanceEvent(tx *gorm.DB, task *Task, finance *TaskFinance, event *TaskFinanceEvent) error {
	if event == nil || task == nil || finance == nil || event.ID == "" {
		return ErrTaskFinanceInvalid
	}
	if event.DeltaQuota != 0 && common.LogConsumeEnabled {
		var user User
		if err := tx.Unscoped().Select("id", "username").Where("id = ?", event.UserID).Take(&user).Error; err != nil {
			return err
		}
		var token Token
		if err := tx.Unscoped().Select("id", "name").Where("id = ?", event.TokenID).Take(&token).Error; err != nil {
			return err
		}
		var snapshot struct {
			Context TaskBillingContext `json:"context"`
		}
		if err := common.Unmarshal(finance.PricingSnapshot, &snapshot); err != nil {
			return err
		}
		other := NewLogOther()
		other.SetPublic("is_task", true)
		other.SetPublic("task_id", task.TaskID)
		var intake TaskIntake
		if err := tx.Select("task_id", "routing_snapshot").Where("task_id = ?", event.TaskID).Take(&intake).Error; err != nil {
			return err
		}
		var routing struct {
			Protocol string `json:"protocol"`
		}
		if err := common.Unmarshal(intake.RoutingSnapshot, &routing); err != nil {
			return err
		}
		if routing.Protocol == "openai_image" {
			other.SetPublic("task_sync", true)
		}
		other.SetPublic("group_ratio", snapshot.Context.GroupRatio)
		if snapshot.Context.ModelPrice > 0 {
			other.SetPublic("model_price", snapshot.Context.ModelPrice)
		}
		if snapshot.Context.ModelRatio > 0 {
			other.SetPublic("model_ratio", snapshot.Context.ModelRatio)
		}
		if snap := snapshot.Context.TieredSnapshot; snap != nil {
			other.SetPublic("billing_mode", "tiered_expr")
			other.SetPublic("expr_b64", base64.StdEncoding.EncodeToString([]byte(snap.ExprString)))
			if snap.EstimatedTier != "" {
				other.SetPublic("estimated_tier", snap.EstimatedTier)
				if event.Kind != "terminal" {
					other.SetPublic("matched_tier", snap.EstimatedTier)
				}
			}
			if len(snap.UsageFacts) > 0 {
				other.SetPublic("usage_facts", safeTaskUsageFacts(snap.UsageFacts))
			}
			setNativeTaskImageCount(other, snap.UsageFacts["image_count"])
		} else {
			setNativeTaskImageCount(other, snapshot.Context.OtherRatios["image_count"])
		}
		if event.Kind == "terminal" {
			other.SetPublic("final_quota", event.FinalQuota)
			other.SetPublic("reserved_quota", event.ReservedQuota)
			var pending PendingTaskOutcome
			if err := tx.Select("task_id", "status", "usage_facts").Where("task_id = ?", event.TaskID).Take(&pending).Error; err != nil {
				return err
			}
			facts := make(map[string]any)
			if snap := snapshot.Context.TieredSnapshot; snap != nil {
				maps.Copy(facts, snap.UsageFacts)
			}
			if len(pending.UsageFacts) > 0 {
				var finalFacts map[string]any
				if err := common.Unmarshal(pending.UsageFacts, &finalFacts); err != nil {
					return err
				}
				maps.Copy(facts, finalFacts)
			}
			if len(facts) > 0 {
				other.SetPublic("usage_facts", safeTaskUsageFacts(facts))
				setNativeTaskImageCount(other, facts["image_count"])
			}
			if snap := snapshot.Context.TieredSnapshot; snap != nil && pending.Status == TaskStatusSuccess {
				final, err := billingexpr.ComputeTieredQuotaWithRequest(snap, billingexpr.TokenParams{}, billingexpr.RequestInput{Usage: facts})
				if err != nil || final.Clamp != nil || final.ActualQuotaAfterGroup != event.FinalQuota {
					return ErrTaskFinanceInvalid
				}
				if final.MatchedTier != "" {
					other.SetPublic("matched_tier", final.MatchedTier)
				}
			}
		}
		kind, quota := LogTypeConsume, event.DeltaQuota
		content := "Task quota reserved"
		if event.Kind == "reserve_adjust" {
			content = "Task reservation adjusted"
		} else if event.Kind == "terminal" {
			content = "Task quota settled"
		}
		if quota < 0 {
			kind, quota, content = LogTypeRefund, -quota, "Task quota refunded"
		}
		log := Log{UserId: event.UserID, Username: user.Username, CreatedAt: event.CreatedAt,
			Type: kind, Content: other.setContent([]*common.Message{common.NewMessage(content)}),
			TokenName: token.Name, ModelName: task.Properties.OriginModelName,
			Quota: quota, ChannelId: event.ChannelID, TokenId: event.TokenID,
			Group: task.Group, RequestId: event.ID, Other: other.JSONString()}
		payload, err := common.Marshal(log)
		if err != nil || len(payload) > 16<<10 {
			return ErrTaskFinanceInvalid
		}
		event.LogRequired = true
		event.LogPayload = TaskBlob(payload)
		hash := sha256.Sum256(payload)
		event.LogHash = hex.EncodeToString(hash[:])
	} else {
		event.ProjectedAt = event.CreatedAt
	}
	if event.Kind == "terminal" && common.DataExportEnabled {
		var user User
		if err := tx.Unscoped().Select("id", "username").Where("id = ?", event.UserID).Take(&user).Error; err != nil {
			return err
		}
		row := QuotaData{UserID: event.UserID, Username: user.Username,
			ModelName: task.Properties.OriginModelName, CreatedAt: event.CreatedAt - event.CreatedAt%3600,
			UseGroup: task.Group, TokenID: event.TokenID, ChannelID: event.ChannelID,
			NodeName: task.PrivateData.NodeName, Count: 1, Quota: event.FinalQuota}
		payload, err := common.Marshal(row)
		if err != nil || len(payload) > 8<<10 {
			return ErrTaskFinanceInvalid
		}
		event.ExportRequired = true
		event.ExportPayload = TaskBlob(payload)
		hash := sha256.Sum256(payload)
		event.ExportHash = hex.EncodeToString(hash[:])
	} else {
		event.ExportedAt = event.CreatedAt
	}
	if len(event.LogPayload) > 0 && len(event.ExportPayload) > 0 &&
		len(event.LogPayload)+len(event.ExportPayload) > 24<<10 {
		return ErrTaskFinanceInvalid
	}
	return tx.Create(event).Error
}

func setNativeTaskImageCount(other *LogOther, value any) {
	count, ok := value.(float64)
	if ok && count >= 0 && count <= float64(dto.MaxImageN) && count == float64(int(count)) {
		other.SetPublic("image_count", int(count))
	}
}

func safeTaskUsageFacts(facts map[string]any) map[string]any {
	if len(facts) > 32 {
		return nil
	}
	safe := make(map[string]any, len(facts))
	for key, value := range facts {
		if len(key) == 0 || len(key) > 64 {
			continue
		}
		for _, char := range key {
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '_' {
				key = ""
				break
			}
		}
		if key == "" {
			continue
		}
		switch typed := value.(type) {
		case bool, float64, int, int64:
			safe[key] = typed
		case string:
			allowed := key == "mode" || key == "quality" || key == "resolution" || key == "size" || key == "tier"
			if allowed && len(typed) > 0 && len(typed) <= 32 {
				for _, char := range typed {
					if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') &&
						(char < '0' || char > '9') && char != '_' && char != '-' {
						allowed = false
						break
					}
				}
			}
			if allowed && len(typed) > 0 && len(typed) <= 32 {
				safe[key] = typed
			}
		}
	}
	return safe
}

func ListDueTaskFinanceEvents(ctx context.Context, afterID string, limit int) ([]TaskFinanceEvent, error) {
	if limit < 1 || limit > 100 {
		limit = 16
	}
	var events []TaskFinanceEvent
	now := time.Now().Unix()
	err := DB.WithContext(ctx).Where("id > ? AND ((projected_at = 0 AND log_next_at <= ?) OR (exported_at = 0 AND export_next_at <= ?))", afterID, now, now).
		Order("id asc").Limit(limit).Find(&events).Error
	return events, err
}

func HasTaskRecoveryBacklog(ctx context.Context) (bool, error) {
	var count int64
	if err := DB.WithContext(ctx).Model(&PendingTaskOutcome{}).Where("state = ?", "pending").Limit(1).Count(&count).Error; err != nil || count > 0 {
		return count > 0, err
	}
	if err := DB.WithContext(ctx).Model(&TaskFinanceEvent{}).Where("projected_at = 0 OR exported_at = 0").Limit(1).Count(&count).Error; err != nil || count > 0 {
		return count > 0, err
	}
	return HasTaskWebhookBacklog(ctx)
}

func projectionBackoff(failures int) int64 {
	return min(int64(30)<<min(failures, 7), int64(3600))
}

func deferTaskFinanceProjection(ctx context.Context, eventID, kind string, failures int) {
	column, due := "log_failures", "log_next_at"
	if kind == "export" {
		column, due = "export_failures", "export_next_at"
	}
	_ = DB.WithContext(ctx).Model(&TaskFinanceEvent{}).Where("id = ?", eventID).
		Updates(map[string]any{column: failures + 1, due: time.Now().Unix() + projectionBackoff(failures)}).Error
}

func ProjectTaskFinanceLog(ctx context.Context, event TaskFinanceEvent) (err error) {
	if event.ProjectedAt != 0 {
		return nil
	}
	defer func() {
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			deferTaskFinanceProjection(cleanupCtx, event.ID, "log", event.LogFailures)
		}
	}()
	if !event.LogRequired || len(event.LogPayload) == 0 ||
		fmt.Sprintf("%x", sha256.Sum256(event.LogPayload)) != event.LogHash ||
		common.UsingLogDatabase(common.DatabaseTypeClickHouse) {
		return errors.New("native task log projection is unavailable")
	}
	var expected Log
	if err = common.Unmarshal(event.LogPayload, &expected); err != nil {
		return err
	}
	expected.NativeEventID = &event.ID
	if err = LOG_DB.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&expected).Error; err != nil {
		return err
	}
	// Readback is a fresh statement, outside a failed PostgreSQL insert transaction.
	var stored Log
	if err = LOG_DB.WithContext(ctx).Where("native_event_id = ?", event.ID).Take(&stored).Error; err != nil {
		return err
	}
	stored.Id, expected.Id = 0, 0
	stored.ChannelName = ""
	if !reflect.DeepEqual(stored, expected) {
		return errors.New("native task log identity has a different immutable payload")
	}
	result := DB.WithContext(ctx).Model(&TaskFinanceEvent{}).
		Where("id = ? AND projected_at = 0 AND log_hash = ?", event.ID, event.LogHash).
		Update("projected_at", time.Now().Unix())
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrTaskFinanceConflict
	}
	return nil
}

func ProjectTaskFinanceExport(ctx context.Context, event TaskFinanceEvent) (err error) {
	if event.ExportedAt != 0 {
		return nil
	}
	defer func() {
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			deferTaskFinanceProjection(cleanupCtx, event.ID, "export", event.ExportFailures)
		}
	}()
	if !event.ExportRequired || len(event.ExportPayload) == 0 ||
		fmt.Sprintf("%x", sha256.Sum256(event.ExportPayload)) != event.ExportHash {
		return errors.New("native task dashboard projection is unavailable")
	}
	var expected QuotaData
	if err = common.Unmarshal(event.ExportPayload, &expected); err != nil {
		return err
	}
	expected.NativeEventID = &event.ID
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&expected).Error; err != nil {
			return err
		}
		var stored QuotaData
		if err := tx.Where("native_event_id = ?", event.ID).Take(&stored).Error; err != nil {
			return err
		}
		stored.Id, expected.Id = 0, 0
		if !reflect.DeepEqual(stored, expected) {
			return errors.New("native task dashboard identity has a different immutable payload")
		}
		result := tx.Model(&TaskFinanceEvent{}).
			Where("id = ? AND exported_at = 0 AND export_hash = ?", event.ID, event.ExportHash).
			Update("exported_at", time.Now().Unix())
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskFinanceConflict
		}
		return nil
	})
}
