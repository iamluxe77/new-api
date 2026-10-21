package service

import (
	"context"
	"errors"
	"math"
	"math/big"
	"strconv"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/jsplugin"
	"gorm.io/gorm"
)

// BuildDurableVideoTask is the terminal public snapshot used by both an
// authorized poll and a callback. Native state and times always win over a
// plugin's presentation of the same fields.
func BuildDurableVideoTask(ctx context.Context, task *model.Task, intake *model.TaskIntake) (map[string]any, error) {
	if task == nil || intake == nil || intake.TaskID != task.ID {
		return nil, errors.New("durable video snapshot is unavailable")
	}
	var frozen model.TaskWebhookDelivery
	if err := model.DB.WithContext(ctx).Select("task_id", "public_body").Where("task_id = ? AND body_hash <> ?", task.ID, "").Take(&frozen).Error; err == nil && len(frozen.PublicBody) > 0 {
		var public map[string]any
		if err := common.Unmarshal(frozen.PublicBody, &public); err != nil {
			return nil, err
		}
		view, err := BuildDurableTaskPluginView(ctx, task)
		if err != nil {
			return nil, err
		}
		metadata, _ := public["metadata"].(map[string]any)
		if metadata == nil {
			metadata = make(map[string]any)
		}
		metadata["lifecycle"] = view.Lifecycle
		public["metadata"] = metadata
		return public, nil
	} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	baseBytes, err := common.Marshal(task.ToOpenAIVideo())
	if err != nil {
		return nil, err
	}
	var base map[string]any
	if err := common.Unmarshal(baseBytes, &base); err != nil {
		return nil, err
	}
	base["id"] = task.TaskID
	base["model"] = task.Properties.OriginModelName
	base["created_at"] = task.CreatedAt
	base["updated_at"] = task.UpdatedAt
	base["completed_at"] = nil
	if task.Status == model.TaskStatusSuccess || task.Status == model.TaskStatusFailure {
		base["completed_at"] = task.FinishTime
	}
	base["seconds"], base["size"], base["duration"], base["generate_audio"] = nil, nil, nil, nil
	base["content"], base["error"] = nil, nil
	if task.Status == model.TaskStatusFailure {
		code := task.PrivateData.ResultErrorCode
		if code == "" {
			code = "task_failed"
		}
		base["error"] = map[string]any{"code": code, "message": model.TaskPublicFailureMessage(code)}
	}
	var frozenBilling DurableTaskBillingSnapshot
	if err := common.Unmarshal(intake.BillingSnapshot, &frozenBilling); err != nil {
		return nil, err
	}
	requestID := any(nil)
	if task.PrivateData.Execution != nil && task.PrivateData.Execution.RequestID != "" {
		requestID = task.PrivateData.Execution.RequestID
	}
	pricing := map[string]any{"billing_mode": nil, "group_ratio": nil, "model_price": nil,
		"model_ratio": nil, "billing_unit": nil, "estimated_fixed_price": nil,
		"reservation_multiplier": nil, "estimated_tier": nil, "quota_per_unit": nil,
		"estimated_quota_before_group": nil, "estimated_quota_after_group": nil}
	if frozenBilling.Context.GroupRatio > 0 {
		pricing["group_ratio"] = strconv.FormatFloat(frozenBilling.Context.GroupRatio, 'f', -1, 64)
	}
	if frozenBilling.Context.ModelPrice > 0 {
		pricing["model_price"] = strconv.FormatFloat(frozenBilling.Context.ModelPrice, 'f', -1, 64)
	}
	if frozenBilling.Context.ModelRatio > 0 {
		pricing["model_ratio"] = strconv.FormatFloat(frozenBilling.Context.ModelRatio, 'f', -1, 64)
	}
	quotaPerUnit := frozenBilling.QuotaPerUnit
	if snapshot := frozenBilling.Context.TieredSnapshot; snapshot != nil {
		quotaPerUnit = snapshot.QuotaPerUnit
		pricing["billing_mode"] = snapshot.BillingMode
		pricing["billing_unit"] = snapshot.EstimatedBillingUnit
		if snapshot.EstimatedFixedPrice != nil {
			pricing["estimated_fixed_price"] = strconv.FormatFloat(*snapshot.EstimatedFixedPrice, 'f', -1, 64)
		}
		if snapshot.PreConsumeMultiplier > 0 {
			pricing["reservation_multiplier"] = strconv.FormatFloat(snapshot.PreConsumeMultiplier, 'f', -1, 64)
		}
		if snapshot.EstimatedTier != "" {
			pricing["estimated_tier"] = snapshot.EstimatedTier
		}
		pricing["estimated_quota_before_group"] = strconv.FormatFloat(snapshot.EstimatedQuotaBeforeGroup, 'f', -1, 64)
		pricing["estimated_quota_after_group"] = strconv.Itoa(snapshot.EstimatedQuotaAfterGroup)
		if snapshot.GroupRatio > 0 {
			pricing["group_ratio"] = strconv.FormatFloat(snapshot.GroupRatio, 'f', -1, 64)
		}
	}
	if quotaPerUnit > 0 && !math.IsNaN(quotaPerUnit) && !math.IsInf(quotaPerUnit, 0) {
		pricing["quota_per_unit"] = strconv.FormatFloat(quotaPerUnit, 'f', -1, 64)
	}
	base["billing"] = map[string]any{"request_id": requestID, "currency": "USD",
		"reserved_amount": nil, "usage": nil, "pricing": pricing}
	var finance model.TaskFinance
	if err := model.DB.WithContext(ctx).Where("task_id = ?", task.ID).Take(&finance).Error; err != nil {
		return nil, err
	}
	if finance.ReservedQuota < 0 {
		return nil, errors.New("durable reserved quota is invalid")
	}
	if quotaPerUnit > 0 && !math.IsNaN(quotaPerUnit) && !math.IsInf(quotaPerUnit, 0) {
		quota := new(big.Rat).SetInt64(int64(finance.ReservedQuota))
		unit, ok := new(big.Rat).SetString(strconv.FormatFloat(quotaPerUnit, 'f', -1, 64))
		if ok && unit.Sign() > 0 {
			base["billing"].(map[string]any)["reserved_amount"] = quota.Quo(quota, unit).FloatString(6)
		}
	}
	var state struct {
		BillingRequest struct {
			RequestedSeconds *int    `json:"requested_seconds"`
			Resolution       *string `json:"resolution"`
			GenerateAudio    *bool   `json:"generate_audio"`
			VideoCount       *int    `json:"video_count"`
		} `json:"billing_request"`
	}
	if len(task.PrivateData.PluginState) > 0 && common.Unmarshal(task.PrivateData.PluginState, &state) != nil {
		return nil, errors.New("durable billing request is invalid")
	}
	usage := map[string]any{"requested_seconds": state.BillingRequest.RequestedSeconds,
		"output_seconds": nil, "reference_seconds": nil, "resolution": state.BillingRequest.Resolution,
		"reference_media": nil, "generate_audio": state.BillingRequest.GenerateAudio}
	if state.BillingRequest.VideoCount != nil {
		usage["reference_media"] = *state.BillingRequest.VideoCount > 0
	}
	var data struct {
		BillingFacts struct {
			SettlementStatus string  `json:"settlement_status"`
			OutputSeconds    *int    `json:"output_seconds"`
			ReferenceSeconds *int    `json:"reference_seconds"`
			Resolution       *string `json:"resolution"`
		} `json:"billing_facts"`
	}
	if len(task.Data) > 0 && common.Unmarshal(task.Data, &data) != nil {
		return nil, errors.New("durable task data is invalid")
	}
	if data.BillingFacts.SettlementStatus == "settled" {
		usage["output_seconds"] = data.BillingFacts.OutputSeconds
		usage["reference_seconds"] = data.BillingFacts.ReferenceSeconds
		if data.BillingFacts.Resolution != nil {
			usage["resolution"] = data.BillingFacts.Resolution
		}
		if data.BillingFacts.ReferenceSeconds != nil {
			usage["reference_media"] = *data.BillingFacts.ReferenceSeconds > 0
		}
		base["duration"] = data.BillingFacts.OutputSeconds
	}
	if state.BillingRequest.RequestedSeconds != nil && *state.BillingRequest.RequestedSeconds >= 0 {
		base["seconds"] = strconv.Itoa(*state.BillingRequest.RequestedSeconds)
	}
	if state.BillingRequest.GenerateAudio != nil {
		base["generate_audio"] = *state.BillingRequest.GenerateAudio
	}
	if state.BillingRequest.RequestedSeconds != nil || state.BillingRequest.Resolution != nil || state.BillingRequest.GenerateAudio != nil {
		base["billing"].(map[string]any)["usage"] = usage
	}
	if task.Status == model.TaskStatusSuccess || task.Status == model.TaskStatusFailure {
		_, plugin, err := LoadFrozenTaskPlugin(ctx, task.ID)
		if err != nil {
			return nil, err
		}
		routing, err := DecodeDurableRoutingSnapshot(intake)
		if err != nil {
			return nil, err
		}
		if task.Status == model.TaskStatusSuccess && routing.Protocol == "openai_video" {
			base["content_url"] = "/v1/videos/" + task.TaskID + "/content"
		}
		if routing.Protocol == "openai_video" && len(routing.ProtocolRequest) != 0 {
			var request jsplugin.ProtocolRequestContext
			if err := common.Unmarshal(routing.ProtocolRequest, &request); err != nil {
				return nil, err
			}
			view, err := BuildDurableTaskPluginView(ctx, task)
			if err != nil {
				return nil, err
			}
			viewBytes, err := common.Marshal(view)
			if err != nil {
				return nil, err
			}
			rendered, err := plugin.Engine.CallPath(ctx, "protocols", []string{"openai_video", "render"}, request.JSValue(), jsplugin.RawJSON(viewBytes))
			if err != nil {
				return nil, err
			}
			if result, ok := rendered.(map[string]any); ok {
				for _, key := range []string{"seconds", "size", "duration", "generate_audio", "content", "content_url", "video_url"} {
					if value, exists := result[key]; exists {
						base[key] = value
					}
				}
			}
		}
	}
	view, err := BuildDurableTaskPluginView(ctx, task)
	if err != nil {
		return nil, err
	}
	metadata, _ := base["metadata"].(map[string]any)
	if metadata == nil {
		metadata = make(map[string]any)
	}
	metadata["lifecycle"] = view.Lifecycle
	base["metadata"] = metadata
	encoded, err := common.Marshal(base)
	if err != nil || len(encoded) > 1<<20 {
		return nil, errors.New("durable public video snapshot exceeds limit")
	}
	return base, nil
}

func RenderDurableTaskWebhook(ctx context.Context, taskID int64, eventID string) ([]byte, []byte, error) {
	var task model.Task
	if err := model.DB.WithContext(ctx).Where("id = ?", taskID).Take(&task).Error; err != nil {
		return nil, nil, err
	}
	if task.Status != model.TaskStatusSuccess && task.Status != model.TaskStatusFailure {
		return nil, nil, errors.New("durable task is not settled")
	}
	intake, err := model.GetTaskIntake(ctx, taskID)
	if err != nil {
		return nil, nil, err
	}
	routing, err := DecodeDurableRoutingSnapshot(intake)
	if err != nil {
		return nil, nil, err
	}
	deliveryTask, _, err := DurableTaskDelivery(ctx, &task, routing.RetainResult)
	if err != nil {
		return nil, nil, err
	}
	video, err := BuildDurableVideoTask(ctx, deliveryTask, intake)
	if err != nil {
		return nil, nil, err
	}
	publicBody, err := common.Marshal(video)
	if err != nil {
		return nil, nil, err
	}
	_ = eventID // The native delivery header carries this identity.
	return publicBody, publicBody, nil
}
