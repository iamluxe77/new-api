package model

import (
	"context"
	"time"

	"gorm.io/gorm"
)

type TaskRecoveryAggregate struct {
	Stage    string `json:"stage"`
	Count    int64  `json:"count"`
	OldestAt int64  `json:"oldest_at"`
}

// TaskRecoveryAggregates exposes only fixed states and aggregate ages. A failed
// query aborts the response; callers must never interpret it as zero backlog.
func TaskRecoveryAggregates(ctx context.Context) ([]TaskRecoveryAggregate, error) {
	result := make([]TaskRecoveryAggregate, 0, 19)
	appendStage := func(query *gorm.DB, stage, timeColumn string) error {
		var value struct {
			Count    int64
			OldestAt int64
		}
		if err := query.Select("count(*) AS count, coalesce(min(" + timeColumn + "), 0) AS oldest_at").Scan(&value).Error; err != nil {
			return err
		}
		result = append(result, TaskRecoveryAggregate{Stage: stage, Count: value.Count, OldestAt: value.OldestAt})
		return nil
	}
	for _, phase := range []string{TaskDispatchReady, TaskDispatchSendStarted, TaskDispatchAdopting, TaskDispatchUnknown, TaskDispatchAccepted, TaskDispatchParked, TaskDispatchBlocked} {
		if err := appendStage(DB.WithContext(ctx).Model(&TaskDispatch{}).
			Joins("JOIN tasks ON tasks.id = task_dispatches.task_id").
			Where("task_dispatches.phase = ? AND tasks.status NOT IN ?", phase, []TaskStatus{TaskStatusSuccess, TaskStatusFailure}),
			"dispatch_"+phase, "task_dispatches.stage_at"); err != nil {
			return nil, err
		}
	}
	if err := appendStage(DB.WithContext(ctx).Model(&TaskDispatch{}).
		Joins("JOIN tasks t ON t.id = task_dispatches.task_id").
		Where("task_dispatches.phase = ? AND task_dispatches.uncertain_retry_enabled = ? AND task_dispatches.uncertain_replacement_used = ? AND t.status NOT IN ?",
			TaskDispatchUnknown, true, false, []TaskStatus{TaskStatusSuccess, TaskStatusFailure}),
		"uncertain_replacement_pending", "task_dispatches.stage_at"); err != nil {
		return nil, err
	}
	if err := appendStage(DB.WithContext(ctx).Model(&TaskDispatchAttempt{}).
		Joins("JOIN task_dispatches d ON d.task_id = task_dispatch_attempts.task_id").
		Where("d.uncertain_replacement_used = ? AND task_dispatch_attempts.watch_state = ?", true, "pending"),
		"uncertain_original_tail_pending", "task_dispatch_attempts.started_at / 1000"); err != nil {
		return nil, err
	}
	if err := appendStage(DB.WithContext(ctx).Model(&TaskDispatchAttempt{}).
		Joins("JOIN task_dispatches d ON d.task_id = task_dispatch_attempts.task_id").
		Where("d.uncertain_replacement_used = ? AND d.winner_ordinal > 0 AND task_dispatch_attempts.ordinal <> d.winner_ordinal AND task_dispatch_attempts.watch_state = ? AND task_dispatch_attempts.watch_result_status = ?",
			true, "observed", TaskStatusSuccess),
		"uncertain_late_second_result", "task_dispatch_attempts.watch_observed_at"); err != nil {
		return nil, err
	}
	{
		query := DB.WithContext(ctx).Model(&TaskDispatchAttempt{}).
			Joins("JOIN task_dispatches d ON d.task_id = task_dispatch_attempts.task_id AND d.attempt = task_dispatch_attempts.ordinal").
			Joins("JOIN tasks t ON t.id = d.task_id").
			Where("d.phase IN ? AND t.status NOT IN ? AND task_dispatch_attempts.replay_until > 0 AND task_dispatch_attempts.receipt_ever_seen = ? AND task_dispatch_attempts.receipt_id = ?",
				[]string{TaskDispatchSendStarted, TaskDispatchUnknown}, []TaskStatus{TaskStatusSuccess, TaskStatusFailure}, false, "")
		query = query.Where("task_dispatch_attempts.replay_until <= ?", time.Now().UnixMilli())
		if err := appendStage(query, "replay_expired", "task_dispatch_attempts.started_at / 1000"); err != nil {
			return nil, err
		}
	}
	if err := appendStage(DB.WithContext(ctx).Model(&TaskIntake{}).Joins("JOIN tasks ON tasks.id = task_intakes.task_id").
		Where("tasks.status NOT IN ?", []TaskStatus{TaskStatusSuccess, TaskStatusFailure}).
		Where("NOT EXISTS (SELECT 1 FROM task_dispatches WHERE task_dispatches.task_id = task_intakes.task_id)"),
		"intake", "tasks.created_at"); err != nil {
		return nil, err
	}
	if err := appendStage(DB.WithContext(ctx).Model(&TaskHTTPObservation{}).
		Joins("JOIN task_dispatches ON task_dispatches.task_id = task_http_observations.task_id").
		Where("task_dispatches.phase <> ?", TaskDispatchBlocked), "observation_awaiting_parse", "observed_at"); err != nil {
		return nil, err
	}
	if err := appendStage(DB.WithContext(ctx).Model(&PendingTaskOutcome{}).Where("state = ?", "pending"), "finance_pending", "created_at"); err != nil {
		return nil, err
	}
	if err := appendStage(DB.WithContext(ctx).Model(&TaskFinanceEvent{}).Where("projected_at = 0"), "finance_log_pending", "created_at"); err != nil {
		return nil, err
	}
	if err := appendStage(DB.WithContext(ctx).Model(&TaskFinanceEvent{}).Where("exported_at = 0"), "dashboard_pending", "created_at"); err != nil {
		return nil, err
	}
	for _, state := range []string{"pending", "blocked", "expired"} {
		if err := appendStage(DB.WithContext(ctx).Model(&TaskWebhookDelivery{}).Where("state = ?", state), "webhook_"+state, "created_at"); err != nil {
			return nil, err
		}
	}
	return result, nil
}
