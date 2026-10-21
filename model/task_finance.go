package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
)

var (
	ErrTaskFinanceConflict      = errors.New("task finance version or outcome conflict")
	ErrTaskQuotaInsufficient    = errors.New("task quota insufficient")
	ErrTaskFinanceInvalid       = errors.New("invalid task finance request")
	ErrTaskCounterCapacity      = errors.New("task counter exceeds database column capacity")
	ErrTaskFinancePeriodChanged = errors.New("task subscription period changed; original-period settlement pending")
)

type TaskReservation struct {
	TokenID int
	Quota   int
	// Legacy caller hints are ignored. The transaction selects funding and token state.
	Source         string
	SubscriptionID int
	TokenUnlimited bool
}

type TaskFinance struct {
	TaskID          int64 `gorm:"primaryKey;autoIncrement:false"`
	UserID          int   `gorm:"index"`
	TokenID         int
	TokenUnlimited  bool
	SubscriptionID  int
	Source          string `gorm:"type:varchar(20);not null"`
	ReservedQuota   int
	ActualQuota     int
	Version         int64
	State           string   `gorm:"type:varchar(20);not null;index"`
	OutcomeHash     string   `gorm:"type:char(64)"`
	PricingSnapshot TaskBlob `json:"-"`
	PlanID          int
	PeriodStart     int64
	PeriodEnd       int64
	LastResetTime   int64
	NextResetTime   int64
}

type TaskFinanceEvent struct {
	ID             string `gorm:"primaryKey;type:varchar(100)"`
	TaskID         int64  `gorm:"index"`
	Kind           string `gorm:"type:varchar(30)"`
	Quota          int
	DeltaQuota     int
	ReservedQuota  int
	FinalQuota     int
	FinanceVersion int64
	UserID         int
	TokenID        int
	ChannelID      int
	Source         string `gorm:"type:varchar(20)"`
	SubscriptionID int
	PeriodStart    int64
	PeriodEnd      int64
	CreatedAt      int64
	ProjectedAt    int64    `gorm:"index"`
	LogPayload     TaskBlob `json:"-"`
	LogHash        string   `json:"-" gorm:"type:char(64)"`
	LogRequired    bool     `json:"-"`
	LogNextAt      int64    `json:"-" gorm:"index"`
	LogFailures    int      `json:"-"`
	ExportPayload  TaskBlob `json:"-"`
	ExportHash     string   `json:"-" gorm:"type:char(64)"`
	ExportRequired bool     `json:"-"`
	ExportedAt     int64    `gorm:"index"`
	ExportNextAt   int64    `json:"-" gorm:"index"`
	ExportFailures int      `json:"-"`
}

type TaskOutcome struct {
	Status       TaskStatus
	ActualQuota  int
	OutcomeHash  string
	TaskID       string
	Data         []byte
	ResultURL    string
	PluginState  []byte
	UsageFacts   []byte
	RetainResult bool
	ErrorCode    string
	FailReason   string
}

// ListPendingTaskFinances includes all reserved tasks, including tasks that
// have not received a provider outcome. Recovery workers use the separate
// ListPendingTaskOutcomes API to avoid blocking behind these tasks.
func ListPendingTaskFinances(ctx context.Context, limit int) ([]TaskFinance, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var finances []TaskFinance
	err := DB.WithContext(ctx).Where("state <> ?", "settled").Order("task_id asc").Limit(limit).Find(&finances).Error
	return finances, err
}

func ListPendingTaskFinanceEvents(ctx context.Context, limit int) ([]TaskFinanceEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var events []TaskFinanceEvent
	err := DB.WithContext(ctx).Where("projected_at = ?", 0).Order("created_at asc, id asc").Limit(limit).Find(&events).Error
	return events, err
}

func canonicalTaskOutcomeHash(taskID, version int64, dispatch TaskDispatch, outcome TaskOutcome) string {
	h := sha256.New()
	write := func(value []byte) {
		_, _ = fmt.Fprintf(h, "%d:", len(value))
		_, _ = h.Write(value)
	}
	write([]byte(fmt.Sprintf("%d", taskID)))
	write([]byte(fmt.Sprintf("%d", version)))
	write([]byte(fmt.Sprintf("%d", dispatch.Attempt)))
	write([]byte(dispatch.RequestHash))
	write([]byte(dispatch.UpstreamID))
	write([]byte(outcome.Status))
	write([]byte(fmt.Sprintf("%d", outcome.ActualQuota)))
	write([]byte(outcome.TaskID))
	write(outcome.Data)
	write([]byte(outcome.ResultURL))
	write(outcome.PluginState)
	write(outcome.UsageFacts)
	write([]byte(fmt.Sprintf("%t", outcome.RetainResult)))
	write([]byte(outcome.ErrorCode))
	write([]byte(outcome.FailReason))
	return hex.EncodeToString(h.Sum(nil))
}

func reserveTaskFinance(tx *gorm.DB, task *Task, reservation TaskReservation, pricingSnapshot []byte) error {
	if reservation.Quota < 0 || reservation.Quota > common.MaxQuota || reservation.TokenID <= 0 || common.BatchUpdateEnabled {
		return ErrTaskFinanceInvalid
	}
	amount := reservation.Quota
	var user User
	if err := lockForUpdate(tx).Select("id", "status", "quota", "setting").Where("id = ?", task.UserId).First(&user).Error; err != nil {
		return err
	}
	if user.Status != common.UserStatusEnabled {
		return ErrTaskFinanceInvalid
	}
	pref := common.NormalizeBillingPreference(user.GetSetting().BillingPreference)
	now := common.GetTimestamp()
	var subs []UserSubscription
	if pref != "wallet_only" {
		if err := lockForUpdate(tx).Where("user_id = ? AND status = ? AND end_time > ?", task.UserId, "active", now).
			Order("end_time asc, id asc").Find(&subs).Error; err != nil {
			return err
		}
	}
	source := "wallet"
	var selected UserSubscription
	subAmount := max(amount, 1)
	chooseSub := func() (bool, error) {
		for _, candidate := range subs {
			sub := candidate
			var plan SubscriptionPlan
			if err := tx.Where("id = ?", sub.PlanId).First(&plan).Error; err != nil {
				return false, err
			}
			plan.NormalizeDefaults()
			if err := maybeResetUserSubscriptionWithPlanTx(tx, &sub, &plan, now); err != nil {
				return false, err
			}
			if sub.AmountUsed < 0 || sub.AmountUsed > math.MaxInt64-int64(subAmount) {
				return false, ErrTaskCounterCapacity
			}
			if sub.AmountTotal > 0 && sub.AmountTotal-sub.AmountUsed < int64(subAmount) {
				continue
			}
			selected = sub
			return true, nil
		}
		return false, nil
	}
	walletOK := user.Quota > 0 && user.Quota >= amount
	switch pref {
	case "wallet_only":
		if !walletOK {
			return ErrTaskQuotaInsufficient
		}
	case "subscription_only":
		ok, err := chooseSub()
		if err != nil {
			return err
		}
		if !ok {
			return ErrTaskQuotaInsufficient
		}
		source = "subscription"
	case "wallet_first":
		if !walletOK {
			ok, err := chooseSub()
			if err != nil {
				return err
			}
			if !ok {
				return ErrTaskQuotaInsufficient
			}
			source = "subscription"
		}
	default:
		if len(subs) > 0 {
			ok, err := chooseSub()
			if err != nil {
				return err
			}
			if ok {
				source = "subscription"
			} else {
				for _, sub := range subs {
					if !sub.AllowWalletOverflow {
						return ErrTaskQuotaInsufficient
					}
				}
				if !walletOK {
					return ErrTaskQuotaInsufficient
				}
			}
		} else if !walletOK {
			return ErrTaskQuotaInsufficient
		}
	}
	if source == "wallet" {
		if amount > 0 {
			result := tx.Model(&User{}).Where("id = ? AND quota >= ?", task.UserId, amount).Update("quota", gorm.Expr("quota - ?", amount))
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrTaskQuotaInsufficient
			}
		}
	} else {
		amount = subAmount
		result := tx.Model(&UserSubscription{}).Where("id = ? AND amount_used <= ?", selected.Id, math.MaxInt64-int64(amount)).
			Update("amount_used", gorm.Expr("amount_used + ?", amount))
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskCounterCapacity
		}
		if err := tx.Create(&SubscriptionPreConsumeRecord{RequestId: fmt.Sprintf("native-task-%d", task.ID), TaskID: task.ID, UserId: task.UserId,
			UserSubscriptionId: selected.Id, PreConsumed: int64(amount), Status: "consumed"}).Error; err != nil {
			return err
		}
	}
	var token Token
	if err := lockForUpdate(tx).Where("id = ? AND user_id = ?", reservation.TokenID, task.UserId).First(&token).Error; err != nil {
		return err
	}
	if token.Status != common.TokenStatusEnabled || (token.ExpiredTime > 0 && token.ExpiredTime <= common.GetTimestamp()) {
		return ErrTaskFinanceInvalid
	}
	if amount == 0 {
		// Identity and funding are frozen; no balance mutation is needed.
	} else if token.UnlimitedQuota {
		result := tx.Model(&Token{}).Where("id = ? AND user_id = ? AND used_quota <= ? AND remain_quota >= ?", token.Id, task.UserId, math.MaxInt64-int64(amount), math.MinInt64+int64(amount)).
			Updates(map[string]any{"remain_quota": gorm.Expr("remain_quota - ?", amount), "used_quota": gorm.Expr("used_quota + ?", amount)})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskQuotaInsufficient
		}
	} else {
		result := tx.Model(&Token{}).Where("id = ? AND user_id = ? AND remain_quota >= ? AND used_quota <= ?", token.Id, task.UserId, amount, math.MaxInt64-int64(amount)).
			Updates(map[string]any{"remain_quota": gorm.Expr("remain_quota - ?", amount), "used_quota": gorm.Expr("used_quota + ?", amount)})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskQuotaInsufficient
		}
	}
	finance := &TaskFinance{TaskID: task.ID, UserID: task.UserId, TokenID: token.Id, SubscriptionID: selected.Id,
		Source: source, TokenUnlimited: token.UnlimitedQuota, ReservedQuota: amount,
		PlanID: selected.PlanId, PeriodStart: max(selected.StartTime, selected.LastResetTime), PeriodEnd: selected.EndTime,
		LastResetTime: selected.LastResetTime, NextResetTime: selected.NextResetTime,
		Version: 1, State: "reserved", PricingSnapshot: TaskBlob(pricingSnapshot)}
	if err := tx.Create(finance).Error; err != nil {
		return err
	}
	return createTaskFinanceEvent(tx, task, finance, &TaskFinanceEvent{ID: fmt.Sprintf("native-task-%d-reserve-v1", task.ID), TaskID: task.ID,
		Kind: "reserve", Quota: amount, DeltaQuota: amount, ReservedQuota: amount,
		FinanceVersion: 1, UserID: task.UserId, TokenID: token.Id, ChannelID: task.ChannelId,
		Source: source, SubscriptionID: selected.Id, PeriodStart: finance.PeriodStart, PeriodEnd: finance.PeriodEnd,
		CreatedAt: common.GetTimestamp()})
}

// AdjustTaskReservation raises a prepared task's native financial barrier
// before the first supplier send. The dispatch lease binds the adjustment to
// the worker that prepared the exact request; no source or pricing is changed.
func AdjustTaskReservation(ctx context.Context, taskID, expectedVersion int64, targetQuota int, claim TaskDispatchClaim) (*TaskFinance, error) {
	if taskID <= 0 || expectedVersion <= 0 || targetQuota < 0 || targetQuota > math.MaxInt32 ||
		claim.TaskID != taskID || claim.Owner == "" {
		return nil, ErrTaskFinanceInvalid
	}
	var updated TaskFinance
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", taskID).First(&task).Error; err != nil {
			return err
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskFinanceConflict
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ?", taskID).First(&finance).Error; err != nil {
			return err
		}
		if finance.State != "reserved" || finance.Version != expectedVersion || targetQuota < finance.ReservedQuota {
			return ErrTaskFinanceConflict
		}
		hasOutcome, err := taskHasOutcomeTx(tx, taskID)
		if err != nil {
			return err
		}
		if hasOutcome {
			return ErrTaskFinanceConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", taskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Phase != TaskDispatchReady || dispatch.Attempt != 0 || dispatch.Version != claim.Version ||
			dispatch.Owner != claim.Owner || dispatch.LeaseUntil <= time.Now().UnixMilli() {
			return ErrTaskDispatchConflict
		}
		if targetQuota == finance.ReservedQuota {
			updated = finance
			return nil
		}
		delta := targetQuota - finance.ReservedQuota
		var user User
		if err := lockForUpdate(tx).Select("id").Where("id = ?", finance.UserID).First(&user).Error; err != nil {
			return err
		}
		if finance.Source == "subscription" {
			var sub UserSubscription
			if err := lockForUpdate(tx).Where("id = ? AND user_id = ?", finance.SubscriptionID, finance.UserID).First(&sub).Error; err != nil {
				return err
			}
			if err := refreshTaskFinanceSubscription(tx, &sub); err != nil {
				return err
			}
			if !taskFinancePeriodMatches(finance, sub) || sub.Status != "active" || sub.EndTime <= common.GetTimestamp() {
				return ErrTaskFinancePeriodChanged
			}
		}
		if finance.Source == "wallet" {
			result := tx.Model(&User{}).Where("id = ? AND quota >= ?", finance.UserID, delta).
				Update("quota", gorm.Expr("quota - ?", delta))
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrTaskQuotaInsufficient
			}
		} else {
			result := tx.Model(&UserSubscription{}).
				Where("id = ? AND user_id = ? AND status = ? AND end_time > ? AND amount_used <= ? AND (amount_total = 0 OR amount_used <= amount_total - ?)",
					finance.SubscriptionID, finance.UserID, "active", common.GetTimestamp(), math.MaxInt64-int64(delta), delta).
				Update("amount_used", gorm.Expr("amount_used + ?", delta))
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrTaskQuotaInsufficient
			}
			result = tx.Model(&SubscriptionPreConsumeRecord{}).
				Where("request_id = ? AND status = ? AND pre_consumed <= ?", fmt.Sprintf("native-task-%d", taskID), "consumed", math.MaxInt64-int64(delta)).
				Update("pre_consumed", gorm.Expr("pre_consumed + ?", delta))
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrTaskFinanceConflict
			}
		}
		var token Token
		if err := lockForUpdate(tx).Where("id = ? AND user_id = ?", finance.TokenID, finance.UserID).First(&token).Error; err != nil {
			return err
		}
		if finance.TokenUnlimited {
			query := tx.Unscoped().Model(&Token{}).Where("id = ? AND used_quota <= ? AND remain_quota >= ?", token.Id, math.MaxInt64-int64(delta), math.MinInt64+int64(delta))
			result := query.Updates(map[string]any{"remain_quota": gorm.Expr("remain_quota - ?", delta), "used_quota": gorm.Expr("used_quota + ?", delta)})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrTaskQuotaInsufficient
			}
		} else {
			result := tx.Model(&Token{}).
				Where("id = ? AND remain_quota >= ? AND used_quota <= ?", token.Id, delta, math.MaxInt64-int64(delta)).
				Updates(map[string]any{"remain_quota": gorm.Expr("remain_quota - ?", delta), "used_quota": gorm.Expr("used_quota + ?", delta)})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrTaskQuotaInsufficient
			}
		}
		result := tx.Model(&TaskFinance{}).Where("task_id = ? AND version = ? AND state = ?", taskID, expectedVersion, "reserved").
			Updates(map[string]any{"reserved_quota": targetQuota, "version": expectedVersion + 1})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrTaskFinanceConflict
		}
		finance.ReservedQuota = targetQuota
		finance.Version++
		if err := createTaskFinanceEvent(tx, &task, &finance, &TaskFinanceEvent{ID: fmt.Sprintf("native-task-%d-reserve-v%d", taskID, finance.Version), TaskID: taskID,
			Kind: "reserve_adjust", Quota: delta, DeltaQuota: delta, ReservedQuota: targetQuota,
			FinanceVersion: finance.Version, UserID: finance.UserID, TokenID: finance.TokenID,
			ChannelID: task.ChannelId, Source: finance.Source, SubscriptionID: finance.SubscriptionID,
			PeriodStart: finance.PeriodStart, PeriodEnd: finance.PeriodEnd, CreatedAt: common.GetTimestamp()}); err != nil {
			return err
		}
		updated = finance
		return nil
	})
	if err != nil {
		return nil, err
	}
	invalidateTaskFinanceCaches(updated.UserID, updated.TokenID)
	return &updated, nil
}

// CommitTaskOutcome owns terminal task state, money, counters and the durable
// projection intent. Replays with the same outcome hash are read-only.
func CommitTaskOutcome(ctx context.Context, taskID, expectedVersion int64, outcome TaskOutcome) error {
	if err := PersistTaskOutcome(ctx, taskID, expectedVersion, outcome); err != nil {
		deferPendingTaskOutcome(ctx, taskID)
		return err
	}
	var userID, tokenID int
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task Task
		if err := lockForUpdate(tx).Where("id = ?", taskID).First(&task).Error; err != nil {
			return err
		}
		var finance TaskFinance
		if err := lockForUpdate(tx).Where("task_id = ?", taskID).First(&finance).Error; err != nil {
			return err
		}
		var pending PendingTaskOutcome
		if err := lockForUpdate(tx).Where("task_id = ?", taskID).First(&pending).Error; err != nil {
			return err
		}
		hash := pending.Hash
		if pending.FinanceVersion != expectedVersion || pending.State != "pending" && pending.State != "settled" ||
			pending.Status != outcome.Status || pending.ActualQuota != outcome.ActualQuota || pending.UpstreamID != outcome.TaskID ||
			pending.ErrorCode != outcome.ErrorCode || pending.FailReason != outcome.FailReason || string(pending.Data) != string(outcome.Data) {
			// Keep the private delivery payload and its public retention decision immutable.
			return ErrTaskFinanceConflict
		}
		if pending.ResultURL != outcome.ResultURL || string(pending.PluginState) != string(outcome.PluginState) ||
			string(pending.UsageFacts) != string(outcome.UsageFacts) ||
			pending.RetainResult != outcome.RetainResult {
			return ErrTaskFinanceConflict
		}
		if finance.State == "settled" {
			if finance.OutcomeHash == hash && finance.ActualQuota == outcome.ActualQuota && task.Status == outcome.Status {
				return nil
			}
			return ErrTaskFinanceConflict
		}
		if finance.State != "reserved" || finance.Version != expectedVersion {
			return ErrTaskFinanceConflict
		}
		if pending.State != "pending" {
			return ErrTaskFinanceConflict
		}
		var dispatch TaskDispatch
		if err := lockForUpdate(tx).Where("task_id = ?", taskID).First(&dispatch).Error; err != nil {
			return err
		}
		if dispatch.Phase == TaskDispatchSendStarted || dispatch.Phase == TaskDispatchUnknown ||
			dispatch.Phase == TaskDispatchParked {
			return ErrTaskFinanceConflict
		}
		if outcome.Status == TaskStatusSuccess && (dispatch.Phase != TaskDispatchAccepted || outcome.TaskID == "" || outcome.TaskID != dispatch.UpstreamID) {
			return ErrTaskFinanceConflict
		}
		if dispatch.Phase == TaskDispatchAccepted && outcome.TaskID != dispatch.UpstreamID {
			return ErrTaskFinanceConflict
		}
		var user User
		if err := lockForUpdate(tx.Unscoped()).Select("id").Where("id = ?", finance.UserID).First(&user).Error; err != nil {
			return err
		}
		delta := outcome.ActualQuota - finance.ReservedQuota
		if finance.Source == "wallet" {
			if delta != 0 {
				query := tx.Unscoped().Model(&User{}).Where("id = ?", finance.UserID)
				if delta > 0 {
					query = query.Where("quota >= ?", -common.MaxWalletQuota+delta)
				} else {
					query = query.Where("quota <= ?", common.MaxWalletQuota+delta)
				}
				result := query.Update("quota", gorm.Expr("quota - ?", delta))
				if result.Error != nil {
					return result.Error
				}
				if result.RowsAffected != 1 {
					return ErrTaskCounterCapacity
				}
			}
		} else {
			var sub UserSubscription
			if err := lockForUpdate(tx).Where("id = ? AND user_id = ?", finance.SubscriptionID, finance.UserID).First(&sub).Error; err != nil {
				return err
			}
			if err := refreshTaskFinanceSubscription(tx, &sub); err != nil {
				return err
			}
			// A positive old-period delta needs explicit reconciliation. A
			// negative delta records the original refund without touching the
			// new period's AmountUsed.
			periodChanged := !taskFinancePeriodMatches(finance, sub)
			if delta > 0 && periodChanged {
				return ErrTaskFinancePeriodChanged
			}
			if delta != 0 && !periodChanged {
				query := tx.Model(&UserSubscription{}).Where("id = ? AND user_id = ? AND amount_used >= ? AND amount_used <= ?", finance.SubscriptionID, finance.UserID, max(-delta, 0), math.MaxInt64-max(delta, 0))
				if delta > 0 {
					query = query.Where("amount_total = 0 OR amount_used <= amount_total - ?", delta)
				}
				result := query.Update("amount_used", gorm.Expr("amount_used + ?", delta))
				if result.Error != nil {
					return result.Error
				}
				if result.RowsAffected != 1 {
					return ErrTaskQuotaInsufficient
				}
			}
			recordResult := tx.Model(&SubscriptionPreConsumeRecord{}).Where("request_id = ? AND status = ?", fmt.Sprintf("native-task-%d", taskID), "consumed").
				Updates(map[string]any{"status": "settled", "pre_consumed": outcome.ActualQuota})
			if recordResult.Error != nil {
				return recordResult.Error
			}
			if recordResult.RowsAffected != 1 {
				return ErrTaskFinanceConflict
			}
		}
		userResult := tx.Unscoped().Model(&User{}).Where("id = ? AND used_quota >= 0 AND used_quota <= ? AND request_count >= 0 AND request_count < ?",
			finance.UserID, math.MaxInt64-int64(outcome.ActualQuota), math.MaxInt32).
			Updates(map[string]any{"used_quota": gorm.Expr("used_quota + ?", outcome.ActualQuota), "request_count": gorm.Expr("request_count + 1")})
		if userResult.Error != nil {
			return userResult.Error
		}
		if userResult.RowsAffected != 1 {
			return ErrTaskCounterCapacity
		}
		var token Token
		if err := lockForUpdate(tx.Unscoped()).Where("id = ? AND user_id = ?", finance.TokenID, finance.UserID).First(&token).Error; err != nil {
			return err
		}
		if delta == 0 {
			// The token was already reserved; no MySQL no-op update is issued.
		} else if finance.TokenUnlimited {
			query := tx.Unscoped().Model(&Token{}).Where("id = ? AND used_quota >= ? AND used_quota <= ? AND remain_quota >= ? AND remain_quota <= ?", token.Id, max(-delta, 0), math.MaxInt64-max(delta, 0), math.MinInt64+max(delta, 0), math.MaxInt64+min(delta, 0))
			result := query.Updates(map[string]any{"remain_quota": gorm.Expr("remain_quota - ?", delta), "used_quota": gorm.Expr("used_quota + ?", delta)})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrTaskQuotaInsufficient
			}
		} else {
			query := tx.Unscoped().Model(&Token{}).Where("id = ? AND used_quota >= ? AND used_quota <= ? AND remain_quota >= ? AND remain_quota <= ?",
				token.Id, max(-delta, 0), math.MaxInt64-max(delta, 0), math.MinInt64+max(delta, 0), math.MaxInt64+min(delta, 0))
			result := query.Updates(map[string]any{"remain_quota": gorm.Expr("remain_quota - ?", delta), "used_quota": gorm.Expr("used_quota + ?", delta)})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrTaskQuotaInsufficient
			}
		}
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			return ErrTaskFinanceConflict
		}
		if outcome.TaskID != "" {
			task.PrivateData.UpstreamTaskID = outcome.TaskID
		}
		task.PrivateData.ResultURL = outcome.ResultURL
		task.PrivateData.PluginState = append(task.PrivateData.PluginState[:0:0], outcome.PluginState...)
		task.PrivateData.ResultDiscarded = !outcome.RetainResult
		task.PrivateData.ResultErrorCode = outcome.ErrorCode
		var billingSnapshot struct {
			Context TaskBillingContext `json:"context"`
		}
		if err := common.Unmarshal(finance.PricingSnapshot, &billingSnapshot); err != nil {
			return err
		}
		task.PrivateData.BillingContext = &billingSnapshot.Context
		var taskData any
		if outcome.RetainResult && len(outcome.Data) > 0 {
			taskData = string(outcome.Data)
		}
		taskResult := tx.Model(&Task{}).Where("id = ? AND status NOT IN ?", taskID, []TaskStatus{TaskStatusSuccess, TaskStatusFailure}).
			Updates(map[string]any{"status": outcome.Status, "quota": outcome.ActualQuota, "private_data": task.PrivateData, "data": taskData,
				"fail_reason": outcome.FailReason, "progress": "100%", "finish_time": time.Now().Unix(), "updated_at": time.Now().Unix()})
		if taskResult.Error != nil {
			return taskResult.Error
		}
		if taskResult.RowsAffected != 1 {
			return ErrTaskFinanceConflict
		}
		financeResult := tx.Model(&TaskFinance{}).Where("task_id = ? AND version = ? AND state = ?", taskID, expectedVersion, "reserved").
			Updates(map[string]any{"version": expectedVersion + 1, "actual_quota": outcome.ActualQuota, "state": "settled", "outcome_hash": hash})
		if financeResult.Error != nil {
			return financeResult.Error
		}
		if financeResult.RowsAffected != 1 {
			return ErrTaskFinanceConflict
		}
		if outcome.ActualQuota > 0 {
			channelResult := tx.Model(&Channel{}).Where("id = ? AND used_quota <= ?", task.ChannelId, math.MaxInt64-int64(outcome.ActualQuota)).
				Update("used_quota", gorm.Expr("used_quota + ?", outcome.ActualQuota))
			if channelResult.Error != nil {
				return channelResult.Error
			}
			if channelResult.RowsAffected != 1 {
				return ErrTaskCounterCapacity
			}
		}
		terminalEventID := fmt.Sprintf("native-task-%d-terminal", taskID)
		if err := createTaskFinanceEvent(tx, &task, &finance, &TaskFinanceEvent{ID: terminalEventID, TaskID: taskID, Kind: "terminal",
			Quota: delta, DeltaQuota: delta, ReservedQuota: finance.ReservedQuota, FinalQuota: outcome.ActualQuota,
			FinanceVersion: expectedVersion + 1, UserID: finance.UserID, TokenID: finance.TokenID,
			ChannelID: task.ChannelId, Source: finance.Source, SubscriptionID: finance.SubscriptionID,
			PeriodStart: finance.PeriodStart, PeriodEnd: finance.PeriodEnd, CreatedAt: time.Now().Unix()}); err != nil {
			return err
		}
		if err := createTaskWebhookDeliveryTx(tx, taskID, terminalEventID, time.Now().Unix()); err != nil {
			return err
		}
		pendingResult := tx.Model(&PendingTaskOutcome{}).Where("task_id = ? AND state = ? AND hash = ?", taskID, "pending", hash).
			Update("state", "settled")
		if pendingResult.Error != nil {
			return pendingResult.Error
		}
		if pendingResult.RowsAffected != 1 {
			return ErrTaskFinanceConflict
		}
		userID, tokenID = finance.UserID, finance.TokenID
		return nil
	})
	if err == nil && userID > 0 {
		invalidateTaskFinanceCaches(userID, tokenID)
	} else if err != nil {
		deferPendingTaskOutcome(ctx, taskID)
	}
	return err
}

func taskFinancePeriodMatches(finance TaskFinance, sub UserSubscription) bool {
	return sub.PlanId == finance.PlanID && sub.EndTime == finance.PeriodEnd &&
		sub.LastResetTime == finance.LastResetTime && sub.NextResetTime == finance.NextResetTime &&
		max(sub.StartTime, sub.LastResetTime) == finance.PeriodStart
}

func refreshTaskFinanceSubscription(tx *gorm.DB, sub *UserSubscription) error {
	var plan SubscriptionPlan
	if err := tx.Where("id = ?", sub.PlanId).First(&plan).Error; err != nil {
		return err
	}
	plan.NormalizeDefaults()
	return maybeResetUserSubscriptionWithPlanTx(tx, sub, &plan, common.GetTimestamp())
}

func invalidateTaskFinanceCaches(userID, tokenID int) {
	if err := invalidateUserCache(userID); err != nil {
		common.SysError(err.Error())
	}
	if tokenID > 0 {
		var token Token
		if err := DB.Select("id", commonKeyCol).Where("id = ?", tokenID).First(&token).Error; err == nil {
			if err := invalidateTokenCacheForMutation(token.Key); err != nil {
				common.SysError(err.Error())
			}
		}
	}
}
