package service

import (
	"context"
	"errors"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	"gorm.io/gorm"
)

// BuildTaskPluginView converts a persisted task into the deliberately narrow
// public shape permitted at JavaScript plugin boundaries.
func BuildTaskPluginView(task *model.Task) (dto.TaskView, error) {
	createdAt := task.CreatedAt
	if createdAt == 0 {
		createdAt = task.SubmitTime
	}
	view := dto.TaskView{
		TaskID:     task.TaskID,
		Platform:   string(task.Platform),
		Status:     string(task.Status),
		Progress:   task.Progress,
		FailReason: task.FailReason,
		CreatedAt:  createdAt,
		UpdatedAt:  task.UpdatedAt,
		FinishedAt: task.FinishTime,
	}
	if len(task.Data) > 0 {
		if err := common.Unmarshal(task.Data, &view.Data); err != nil {
			return dto.TaskView{}, err
		}
		view.Data = replacePrivateTaskID(view.Data, task.PrivateData.UpstreamTaskID, task.TaskID)
	}
	return view, nil
}

func BuildDurableTaskPluginView(ctx context.Context, task *model.Task) (dto.TaskView, error) {
	view, err := BuildTaskPluginView(task)
	if err != nil {
		return view, err
	}
	lifecycle := &dto.TaskLifecycleView{}
	if task.Status == model.TaskStatusSuccess || task.Status == model.TaskStatusFailure {
		lifecycle.Stage, lifecycle.StageAt = "settled", task.FinishTime
	} else {
		var pending model.PendingTaskOutcome
		err := model.DB.WithContext(ctx).Select("task_id", "state", "created_at", "next_attempt_at").
			Where("task_id = ? AND state = ?", task.ID, "pending").Take(&pending).Error
		if err == nil {
			lifecycle.Stage, lifecycle.StageAt, lifecycle.NextAction = "finance_pending", pending.CreatedAt, "settle"
			lifecycle.NextDueAt = pending.NextAttemptAt
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return dto.TaskView{}, err
		} else {
			var dispatch model.TaskDispatch
			if err := model.DB.WithContext(ctx).Select("task_id", "phase", "stage_at", "request_hash", "next_attempt_at").
				Where("task_id = ?", task.ID).Take(&dispatch).Error; err != nil {
				return dto.TaskView{}, err
			}
			lifecycle.Stage, lifecycle.StageAt = dispatch.Phase, dispatch.StageAt
			switch dispatch.Phase {
			case model.TaskDispatchReady:
				if dispatch.RequestHash == "" {
					lifecycle.Stage, lifecycle.NextAction = "prepare", "prepare"
				} else {
					lifecycle.NextAction = "submit"
				}
			case model.TaskDispatchSendStarted, model.TaskDispatchUnknown:
				lifecycle.NextAction = "check_receipt"
			case model.TaskDispatchAccepted:
				lifecycle.NextAction = "query"
			case model.TaskDispatchParked, model.TaskDispatchBlocked:
				lifecycle.NextAction = "operator_review"
			}
			if dispatch.NextAttemptAt > 0 {
				lifecycle.NextDueAt = time.UnixMilli(dispatch.NextAttemptAt).Unix()
			}
		}
	}
	var webhook model.TaskWebhookDelivery
	err = model.DB.WithContext(ctx).Select("task_id", "state", "next_attempt_at").Where("task_id = ?", task.ID).Take(&webhook).Error
	if err == nil {
		lifecycle.Webhook = webhook.State
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return dto.TaskView{}, err
	}
	view.Lifecycle = lifecycle
	return view, nil
}

// replacePrivateTaskID rewrites exact private IDs only in known task-ID fields.
// Map keys and opaque strings, including URLs containing the ID, are preserved.
func replacePrivateTaskID(value any, privateTaskID, publicTaskID string) any {
	if privateTaskID == "" || privateTaskID == publicTaskID {
		return value
	}
	switch typed := value.(type) {
	case []any:
		replaced := make([]any, len(typed))
		for index, item := range typed {
			replaced[index] = replacePrivateTaskID(item, privateTaskID, publicTaskID)
		}
		return replaced
	case map[string]any:
		replaced := make(map[string]any, len(typed))
		for key, item := range typed {
			if (key == "id" || key == "task_id" || key == "taskId") && item == privateTaskID {
				replaced[key] = publicTaskID
				continue
			}
			replaced[key] = replacePrivateTaskID(item, privateTaskID, publicTaskID)
		}
		return replaced
	default:
		return value
	}
}
