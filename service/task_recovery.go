package service

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
)

var pendingOutcomeCursor atomic.Int64
var financeEventCursor atomic.Value

// RunTaskRecoveryPass is a bounded tail of the existing async_task_poll lease.
// Each selected task gets at most one recovery command in this pass.
func RunTaskRecoveryPass(ctx context.Context) bool {
	passCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	complete := true
	seen := make(map[int64]bool, 8)
	cursor := pendingOutcomeCursor.Load()
	outcomes, err := model.ListPendingTaskOutcomesAfter(passCtx, cursor, 3)
	if err != nil {
		logger.LogError(ctx, "task outcome recovery page failed: %v", err)
		return false
	}
	if len(outcomes) == 0 {
		pendingOutcomeCursor.Store(0)
	}
	for _, outcome := range outcomes {
		if passCtx.Err() != nil {
			complete = false
			break
		}
		pendingOutcomeCursor.Store(outcome.TaskID)
		seen[outcome.TaskID] = true
		if err := model.RetryPendingTaskOutcome(passCtx, outcome.TaskID); err != nil {
			logger.LogError(ctx, "task outcome recovery failed: %v", err)
			complete = false
		}
	}
	financeCursor, _ := financeEventCursor.Load().(string)
	events, err := model.ListDueTaskFinanceEvents(passCtx, financeCursor, 8)
	if err != nil {
		logger.LogError(ctx, "task finance projection page failed: %v", err)
		return false
	}
	if len(events) == 0 {
		financeEventCursor.Store("")
	}
	for _, event := range events {
		if passCtx.Err() != nil {
			complete = false
			break
		}
		financeEventCursor.Store(event.ID)
		if seen[event.TaskID] {
			continue
		}
		seen[event.TaskID] = true
		if event.ProjectedAt == 0 && event.LogNextAt <= time.Now().Unix() {
			if err := model.ProjectTaskFinanceLog(passCtx, event); err != nil {
				logger.LogError(ctx, "task finance log projection failed: %v", err)
				complete = false
			}
		} else if event.ExportedAt == 0 && event.ExportNextAt <= time.Now().Unix() {
			if err := model.ProjectTaskFinanceExport(passCtx, event); err != nil {
				logger.LogError(ctx, "task dashboard projection failed: %v", err)
				complete = false
			}
		}
	}
	if !RunTaskWebhookPass(passCtx, seen) {
		complete = false
	}
	return complete && passCtx.Err() == nil
}
